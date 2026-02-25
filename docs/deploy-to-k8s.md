## ВЫКАТКА (в cloud k8s)**
Скрипт пока не используется, выкатывается в ручном режиме

```bash
docker build -f Dockerfile.app -t dmitryst/rosreestr-service-app:latest .
docker build -f Dockerfile.worker -t dmitryst/rosreestr-service-worker:latest .
```

или с флагом --no-cache

```bash
docker build --no-cache -f Dockerfile.app -t dmitryst/rosreestr-service-app:1.0.0 .
docker build --no-cache -f Dockerfile.worker -t dmitryst/rosreestr-service-worker:1.0.5 .

docker push dmitryst/rosreestr-service-app:1.0.0
docker push dmitryst/rosreestr-service-worker:1.0.5

kubectl delete pod <имя-старого-пода>
```
