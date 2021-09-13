# Social Network Serverless

`social-network-serverless` is the serverless version of the social network. Functions and helper functions are developed based on the modified version of Apache OpenWhisk with CPU scheduling support. If you are deploying & running the load test with the vanillia OpenWhisk framework, you need to modify the helper functions in `utils` to disable `cpu-limit` support for function creations and updates.

### Prerequisites
* `pymongo`
* `locust`
* `tqdm`
* `gevent`

### Usage

```bash
docker-compose up -d # create the mongdb containers for backends
python3 social_network.py # create the functions, run the load test with locust
```
