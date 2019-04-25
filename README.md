## Open Targets: Platform batch search worker service
Platform batch search worker service is a dedicated worker processes constantly monitor task queues for running OpenTarget Batch Search returning only the result in the JSON format
https://www.targetvalidation.org/batch-search

##Installation Requirements
* python 2 (celery library)
* git

### Set up application (first time)

- clone repository
- ```cd platform-batch-search-worker-service```
- ```pip install -r requirements.txt``` (possibly in a virtualenv)

### Usage
The workers uses three ENV VARIABLES: <br>
- CELERY_BROKER_URL or default='redis://localhost:6379/0'
- CELERY_RESULT_BACKEND or default='redis://localhost:6379/1'
- CELERY_TASK_RESULT_EXPIRES or default=14400

- ```celery worker -A batch_search_worker --loglevel=info``` runs the workers.


List of tasks available
- ```batch_search_worker.run```
- ```batch_search_worker.ping```
	 


