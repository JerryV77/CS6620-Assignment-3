
# CS6620-Assignment-3

## REST API with databases

This project provides a REST API using `http.server` in Python, integrated with DynamoDB and S3 via Localstack. The API supports CRUD operations for items stored in DynamoDB and S3.

## Prerequisites

- Docker: Make sure Docker is installed on your machine. You can download it from [here](https://www.docker.com/get-started).
- Docker Compose: You can get more information from [here](https://docs.docker.com/compose/).

## Installation

1. **Clone the repository:**

    ```
    git clone https://github.com/JerryV77/CS6620-Assignment-3.git
    ```

2. **Install the necessary dependencies:**

    ```
    pip install -r requirements.txt
    ```

## Running the Stack

To run the stack, execute the following command:

```
./run.sh
```

## Run the tests

To test all the tests, execute the following command:

```
./test.sh
```

Then you will get the following outputs:

```
Run ./test.sh
Creating network "cs6620-assignment-3_default" with the default driver
Creating cs6620-assignment-3_localstack_1 ... 
Creating cs6620-assignment-3_localstack_1 ... done
Creating cs6620-assignment-3_app_1        ... 
Creating cs6620-assignment-3_app_1        ... done
Attaching to cs6620-assignment-3_localstack_1, cs6620-assignment-3_app_1
localstack_1  | 
localstack_1  | LocalStack version: 3.6.1.dev
localstack_1  | LocalStack build date: 2024-08-02
localstack_1  | LocalStack build git hash: 96f447ffc
localstack_1  |
........
app_1         | 
app_1         | ----------------------------------------------------------------------
app_1         | Ran 10 tests in 0.622s
app_1         | 
app_1         | OK
cs6620-assignment-3_app_1 exited with code 0
Stopping cs6620-assignment-3_localstack_1 ... 
Stopping cs6620-assignment-3_localstack_1 ... done
Aborting on container exit...
Removing cs6620-assignment-3_app_1        ... 
Removing cs6620-assignment-3_localstack_1 ... 
Removing cs6620-assignment-3_localstack_1 ... done
Removing cs6620-assignment-3_app_1        ... done
Removing network cs6620-assignment-3_default
```

