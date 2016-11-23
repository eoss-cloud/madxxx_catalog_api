# madxxx_catalog_api
API for the EO catalog system client

starting web client and api in single gunicorn session for dev only
----
- switch to specific virtual env and install missing dependencies from requirements.txt
- set PYTHONPATH var to project directory
- change into catalog.client directory
- execute gunicorn -b :8000 web_client:api --reload

##Instalation of nginx for serving statig files
- brew tap homebrew/nginx
- brew install nginx-full --with-upload-module --with-upload-progress-module
- adapt configuration file
- starting gunicorn in madxxx/ctaog/client: gunicorn -b unix:/tmp/gunicorn.sock  web_client:web --reload --workers 3
- starting nginx: sudo nginx -c /Users/wehrmann/PycharmProjects/madxxx/catalog/client/nginx.conf
- stoping nginx: sudo nginx 

## Docker installation
``` docker run -it -v /tmp/gunicorn:/tmp/gunicorn -e EOSS_CATALOG_DB=SQLALCHEMY_DB_STRING madxxx_catalog ```

## Data harvesting
- Build harvester docker with `sudo docker build -t madxxx_harvester -f harvester.Dockerfile .`
- Check available queues (use your credentials):
 
  ```docker run -it --rm -e AWS_DEFAULT_REGION=xxx -e AWS_ACCESS_KEY_ID=xxx -e AWS_SECRET_ACCESS_KEY=xxx madxxx_harvester queue_list```
 - Start message polling with: