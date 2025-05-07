# A wrapper around a dofn that processes that dofn in an asynchronous manner.
class AsyncWrapper(beam.DoFn):
  TIMER = TimerSpec('timer', TimeDomain.REAL_TIME)
  TIMER_SET = ReadModifyWriteStateSpec('timer_set', coders.BooleanCoder())
  TO_PROCESS = BagStateSpec(
      'to_process',
      coders.TupleCoder([coders.StrUtf8Coder(), coders.StrUtf8Coder()])
  )
  timer_frequency_ = 20
  max_items_to_buffer_ = 20
  processing_elements_ = {}
  items_in_buffer_ = 0
  lock = Lock()

  def __init__(self, sync_fn):
    self._sync_fn = sync_fn
    self._pool = None
    self.fake_bag_state_ = FakeBagState([])
    self._next_time_to_fire = Timestamp.now() + Duration(seconds=5)
    logging.info('async wrapper init')

  def setup(self):
    logging.info('async setup')
    self._sync_fn.setup()

  def teardown(self):
    logging.info('async teardown')
    self._sync_fn.teardown()

  def sync_fn_process(self, element, *args, **kwargs):
    self._sync_fn.start_bundle()
    process_result = self._sync_fn.process(element, *args, **kwargs)
    bundle_result = self._sync_fn.finish_bundle()

    # both process and finish bundle may or may not return generators. We want
    # to combine whatever results have been returned into a single generator. If
    # they are single elements then wrap them in lists so that we can combine
    # them.
    if not isinstance(process_result, GeneratorType):
      process_result = [process_result]
    if not isinstance(bundle_result, GeneratorType):
      bundle_result = [bundle_result]

    # Only count unfinished items in the buffer so at this point we can
    # decrement to unblock accepting new items.
    self.items_in_buffer_ -= 1
    return chain(process_result, bundle_result)

  # Add an item to the processing pool. Add the future returned by that item to
  # processing_elements_.
  def schedule_item(self, element, *args, **kwargs):
    logging.info('scheduling item %s', element)
    if self._pool is None:
      self._pool = ThreadPoolExecutor(max_workers=1)

    result = self._pool.submit(
        lambda: self.sync_fn_process(element, *args, **kwargs),
    )
    with AsyncWrapper.lock:
      AsyncWrapper.processing_elements_[element] = result
      AsyncWrapper.items_in_buffer_ += 1

  def next_time_to_fire(self):
    return (
        floor((time() + self.timer_frequency_) / self.timer_frequency_)
        * self.timer_frequency_
    )

  # Add the incoming element to a pool of elements to process asynchronously.
  def process(
      self,
      element,
      timer=beam.DoFn.TimerParam(TIMER),
      to_process=beam.DoFn.StateParam(TO_PROCESS),
      *args,
      **kwargs
  ):
    # element = "element "+str(element)

    sleep_time = 0
    total_sleep = 0
    while self.items_in_buffer_ > self.max_items_to_buffer_:
      logging.info(
          'buffer is full '
          + str(self.items_in_buffer_)
          + ' waiting 1s.  Have waited for '
          + str(total_sleep)
          + ' seconds.'
      )
      sleep_time += 1
      total_sleep += sleep_time
      sleep(sleep_time)

    self.schedule_item(element)
    to_process.add(element)
    if(running_local):
      self.fake_bag_state_.add(element)

    # Set a timer to fire on the next round increment of timer_frequency_. Note
    # we do this so that each messages timer doesn't get overwritten by the
    # next.
    time_to_fire = self.next_time_to_fire()
    logging.info('setting timer for %s', time_to_fire)
    timer.set(time_to_fire)
    # Timers don't work on local runner so we need to manually trigger the timer
    # callback.
    if(running_local):
      if Timestamp.now() > self._next_time_to_fire:
        return self.timer_callback(self.fake_bag_state_, timer)

    # Don't output any elements.  This will be done in commit_finished_items.
    return []

  # Syncroniseses local state (processing_elements_) with SE state (to_process).
  # Then outputs all finished elements. Finally, sets a timer to fire on the
  # next round increment of timer_frequency_.
  def commit_finished_items(self, to_process=beam.DoFn.StateParam(TO_PROCESS),
                            timer=beam.DoFn.TimerParam(TIMER)):
    # For all elements that are in processing state:
    # If the element is done processing, delete it from all state and yield the
    # output.
    # If the element is not yet done, print it. If the element is not in
    # local state, schedule it for processing.
    items_finished = 0
    items_not_yet_finished = 0
    items_rescheduled = 0
    items_cancelled = 0
    items_in_processing_state = 0
    items_in_se_state = 0

    to_process_local = list(to_process.read())

    # For all elements that in local state but not processing state delete them
    # from local state and cancel their futures.
    to_remove = []
    key = None
    if to_process_local:
      logging.info(
          'check first element of to_process_local:' + str(to_process_local[0])
      )
      key = str(to_process_local[0][0])
    else:
      logging.error(
          'no elements in state during timer callback. Timer should not have'
          ' been set.'
      )
    # processing state is per key so we expect this state to only contain a
    # given key.  Skip items in processing_elements which are for a different
    # key.
    logging.info('key is ' + str(key))
    with AsyncWrapper.lock:
      for x in AsyncWrapper.processing_elements_:
        if x[0] == key and x not in to_process_local:
          items_cancelled += 1
          AsyncWrapper.items_in_buffer_ -= 1
          AsyncWrapper.processing_elements_[x].cancel()
          to_remove.append(x)
          logging.info(
              'cancelling item %s which is no longer in processing state', x
          )
      for x in to_remove:
        AsyncWrapper.processing_elements_.pop(x)

      # For all elements which have finished processing output their result.
      to_return = []
      finished_items = []
      for x in to_process_local:
        logging.info('checking item ' + str(x))
        items_in_se_state += 1
        if x in AsyncWrapper.processing_elements_:
          logging.info('found item '  + str(x))
          if AsyncWrapper.processing_elements_[x].done():
            logging.info('outputting item '  + str(x))
            to_return.append(AsyncWrapper.processing_elements_[x].result())
            finished_items.append(x)
            AsyncWrapper.processing_elements_.pop(x)
            AsyncWrapper.items_in_buffer_ -= 1
            items_finished += 1
          else:
            logging.info('not finished item '  + str(x))
            items_not_yet_finished += 1
        else:
          logging.info(
              'item ' + str(x) + ' found in processing state but not local state'
          )
          self.schedule_item(x)
          items_rescheduled += 1

    # Update processing state to remove elements we've finished
    to_process.clear()
    for x in to_process_local:
      logging.info('iterating through items to delete: '+str(x))
      if x not in finished_items:
        logging.info('item ' + str(x) + ' not finished')
        items_in_processing_state += 1
        to_process.add(x)
      else:
        logging.info('item ' + str(x) + ' finished')

    logging.info('items finished %d', items_finished)
    logging.info('items not yet finished %d', items_not_yet_finished)
    logging.info('items rescheduled %d', items_rescheduled)
    logging.info('items cancelled %d', items_cancelled)
    logging.info('items in processing state %d', items_in_processing_state)
    logging.info('items in buffer %d', self.items_in_buffer_) 

    # If there are items not yet finished then set a timer to fire in the
    # future.
    self._next_time_to_fire = Timestamp.now() + Duration(seconds=5)
    if items_not_yet_finished > 0:
      time_to_fire = self.next_time_to_fire()
      logging.info('setting timer for %s', time_to_fire)
      timer.set(time_to_fire)

    logging.info('outputting ' + str(to_return))

    # Each result is a generator. We want to combine them into a single
    # generator of all elements we wish to output.
    return chain(*to_return)

  @on_timer(TIMER)
  def timer_callback(
      self,
      to_process=beam.DoFn.StateParam(TO_PROCESS),
      timer=beam.DoFn.TimerParam(TIMER),
  ):
    return self.commit_finished_items(to_process, timer)
