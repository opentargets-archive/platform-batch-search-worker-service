## Platform batch search worker service

## Contributing

### Testing locally

- clone repository
- ```cd platform-batch-search-worker-service```
- ```pip install -r requirements.txt``` (possibly in a virtualenv)
- ```celery worker -A batch_search_worker --loglevel=debug``` runs the workers.

The celery worker uses three ENV VARIABLES: <br>
- CELERY_BROKER_URL or default='redis://localhost:6379/0'
- CELERY_RESULT_BACKEND or default='redis://localhost:6379/1'
- CELERY_TASK_RESULT_EXPIRES or default=14400


List of tasks available
- ```batch_search_worker.py.run```
- ```batch_search_worker.py.ping```
	 


