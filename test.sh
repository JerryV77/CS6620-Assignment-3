docker-compose -f docker-compose.test.yaml up --abort-on-container-exit
EXIT_CODE=$(docker-compose -f docker-compose.test.yaml ps -q app | xargs docker inspect -f '{{ .State.ExitCode }}')
docker-compose -f docker-compose.test.yaml down
exit $EXIT_CODE