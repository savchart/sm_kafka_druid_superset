docker compose down
docker rm -f $(docker ps -aq)
docker volume rm -f $(docker volume ls -q)
docker rmi -f $(docker images -aq)
docker system prune -a
