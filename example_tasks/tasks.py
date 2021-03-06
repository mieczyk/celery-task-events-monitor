# Execute worker: celery -A task_events_monitoring.tasks worker --loglevel=INFO -E
# REMEMBER: In order to make worker to send events, the -E (--task-event) option
# must be used ('worker_send_task_events' setting is disabled by default).

import requests
from urllib.parse import urlparse
from celery import Celery
from celery.exceptions import Reject
from bs4 import BeautifulSoup

# Create the Celery application with tasks defined in the 
# task_events_monitoring/tasks.py (this) file.
# The application uses RabbitMQ as the broker and for results' storage.
app = Celery(
    'task_events_monitoring.tasks', 
    broker='pyamqp://guest@localhost', 
    backend='rpc://'
)

# Enable sending the "task-sent" notification.
app.conf.task_send_sent_event=True

@app.task(
    autoretry_for=(requests.HTTPError,), 
    retry_kwargs={ 'max_retries': 3 },
    retry_backoff=True,
    acks_late=True #required if we want to task-rejected event to be sent
)
def visit(url, url_params=[]):
    """
    Visits the given URL address by sending the HTTP GET request.
    It also detects all occurrences of <a> and <img> tags on the 
    given page and store all extracted data in a text file.

    Args:
        url: Full URL address to be visisted.
        url_params: Additional parameters for the HTTP query string.
                    E.g. [('q', 'search phrase')]
    """
    try:
        response = requests.get(url, params=url_params)
        response.raise_for_status()
    except requests.exceptions.ConnectionError as ex:
        raise Reject(ex, requeue=False)

    print('Response from {0}: {1}.'.format(url, response.status_code))

    data = { 'url': url }
    
    # Chain the child tasks execution. First, it calls the 
    # 'extract_data_from_html' task and then it passes results to the
    # 'store_data' task.
    (
        extract_data_from_html.s(response.text, data) 
        | store_data.s()
    ).delay()
    
@app.task
def extract_data_from_html(html, data):
    """
    Uses the Beautiful Soup library to extract all links (href) and 
    images (src) on a given page and stores the results in the 
    dictionary (dict). The dictionary with collected data is returned
    from the task.

    Args:
        html: String with HTML that will be parsed.
        data: dict object that will be complemented by extracted data.

    Returns:
        dict: Dictioanary containing extracted information.
    """
    soup = BeautifulSoup(html, 'html.parser')

    data['links'] = [] 
    data['images'] = []

    for link in soup.find_all('a'):
        data['links'].append(link.get('href'))

    for img in soup.find_all('img'):
        data['images'].append(img.get('src'))

    print('Links and images extracted.')
    return data

@app.task
def store_data(data):
    """
    Saves received data (dict) in a text file.

    Args:
        data: dict object containing data fetched from visited URL.
              Expected format: {'url':'', 'links':[], 'images':[]}.
    """
    url = urlparse(data['url'])
    filename = url.netloc.replace(':', '') + '.txt'

    with open(filename, 'w') as f:
        f.write('=== URL: {}\n'.format(data['url']))
        
        f.write('=== LINKS:\n')
        for link in data['links']:
            f.write(str(link) + '\n')
        
        f.write('=== IMAGES:\n')
        for img in data['images']:
            f.write(str(img) + '\n')
    
    print('Fetched data saved to file {0}'.format(filename))
