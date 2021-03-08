# Overview
This is a very simple monitor that listens for Celery task-related events. It was created on the basis of Celery's official documentation: [Monitoring and Management Guide](https://docs.celeryproject.org/en/stable/userguide/monitoring.html#events).

# Usage
Run the `monitor.py` script on the system where a Celery worker is running and that's it. The script accepts only one optional parameter: `--verbose`. If used, the monitor will display more detailed information fore each captured event.

## Example Celery tasks

You can test the monitor with the example Celery tasks added to this repository. The main task called `visit` is responsible for visiting a given URL and logging all `<a>` and `<img>` HTML tags occurrences on the target page. The example below shows how to start the application and listen for events.

Start the example Celery application with defined tasks. Please note that in order to receive events a Celery worker must be started with the `-E` (`--task-event`) parameter:
```
celery -A example_tasks.tasks worker --loglevel=INFO -E
```

Start the monitor:
```
python monitor.py
```

Run the task:
```
>>> from example_tasks.tasks import visit
>>> visit.delay('https://docs.celeryproject.org')
```