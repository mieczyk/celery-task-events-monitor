# Execute worker: celery -A task_events_monitoring.tasks worker --loglevel=INFO -E
# REMEMBER: In order to make worker to send events the -E (--task-event) option
# must be used ('worker_send_task_events' setting is disabled by default).
# 'task_send_sent_event' setting (since version 2.2) = If enabled, a "task-sent" event will be
# sent for every task so tasks can be tracked before they’re consumed by a worker.

import argparse
from celery import Celery
from datetime import datetime as dt

class Logger:
    def log_task_status_change(self, task, event):
        print('[{}] {} {} (STATE={}, UUID={})'.format(
            self._to_datetime(task.timestamp),
            event['type'].upper(),
            task.name,
            task.state.upper(),
            task.uuid
        ))

    def log_event_details(self, event):
        print('EVENT DETAILS: {}'.format(event))

    def log_task_details(self, task):
        print('TASK DETAILS:')
        print('UUID: {}'.format(task.uuid))
        print('Name: {}'.format(task.name))
        print('State: {}'.format(task.state))
        print('Received: {}'.format(self._to_datetime(task.received)))
        print('Sent: {}'.format(self._to_datetime(task.sent)))
        print('Started: {}'.format(self._to_datetime(task.started)))
        print('Rejected: {}'.format(self._to_datetime(task.rejected)))
        print('Succeeded: {}'.format(self._to_datetime(task.succeeded)))
        print('Failed: {}'.format(self._to_datetime(task.failed)))
        print('Retried: {}'.format(self._to_datetime(task.retried)))
        print('Revoked: {}'.format(self._to_datetime(task.revoked)))
        print('args (arguments): {}'.format(task.args))
        print('kwargs (keyword arguments): {}'.format(task.kwargs))
        print('ETA (Estimated Time of Arrival): {}'.format(task.eta))
        print('Expires: {}'.format(task.expires))
        print('Retries: {}'.format(task.retries))
        print('Worker: {}'.format(task.worker))
        print('Result: {}'.format(task.result))
        print('Exception: {}'.format(task.exception))
        print('Timestamp: {}'.format(self._to_datetime(task.timestamp)))
        print('Runtime: {}'.format(task.runtime))
        print('Traceback: {}'.format(task.traceback))
        print('Exchange: {}'.format(task.exchange))
        print('Routing Key: {}'.format(task.routing_key))
        print('Clock: {}'.format(task.clock))
        print('Client: {}'.format(task.client))
        print('Root: {}'.format(task.root))
        print('Root ID: {}'.format(task.root_id))
        print('Parent: {}'.format(task.parent))
        print('Parent ID: {}'.format(task.parent_id))
        print('Children:')
        for child in task.children:
            print('\t{}\n'.format(str(child)))

    def _to_datetime(self, timestamp):
        return dt.fromtimestamp(timestamp) if timestamp is not None else None 
    
class CeleryEventsHandler:
    def __init__(self, celery_app, verbose_logging=False):
        self._app = celery_app
        self._state = celery_app.events.State()
        self._logger = Logger()
        self._verbose_logging = verbose_logging

    def _event_handler(handler):
        def wrapper(self, event):
            self._state.event(event)
            task = self._state.tasks.get(event['uuid'])
            
            self._logger.log_task_status_change(task, event)
            if(self._verbose_logging):
                self._logger.log_event_details(event)
                self._logger.log_task_details(task)
            
            handler(self, event)
        return wrapper

    @_event_handler
    def _on_task_sent(self, event):
        pass

    @_event_handler
    def _on_task_received(self, event):
        pass

    @_event_handler
    def _on_task_started(self, event):
        pass

    @_event_handler
    def _on_task_succeeded(self, event):
        pass

    @_event_handler
    def _on_task_failed(self, event):
         pass

    @_event_handler
    def _on_task_rejected(self, event):
         pass

    @_event_handler
    def _on_task_revoked(self, event):
         pass

    @_event_handler
    def _on_task_retried(self, event):
        pass

    def start_listening(self):
        with self._app.connection() as connection:
            recv = self._app.events.Receiver(connection, handlers={
                'task-sent': self._on_task_sent,
                'task-received': self._on_task_received,
                'task-started': self._on_task_started,
                'task-succeeded': self._on_task_succeeded,
                'task-failed': self._on_task_failed,
                'task-rejected': self._on_task_rejected,
                'task-revoked': self._on_task_revoked,
                'task-retried': self._on_task_retried
            })
            recv.capture(limit=None, timeout=10)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Dummy monitor of task-related events, generated by a Celery worker.'
    )
    parser.add_argument(
        '--verbose', 
        action='store_true', 
        help='Print detailed information about an event and a related task.'
    )
    args = parser.parse_args()

    # Use RabbitMQ with default credentials as a broker.
    app = Celery(broker='pyamqp://guest@localhost')

    events_handler = CeleryEventsHandler(app, args.verbose)
    events_handler.start_listening()
