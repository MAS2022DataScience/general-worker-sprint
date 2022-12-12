docker build -t spring-general-worker-sprint:0.0.1 -t spring-general-worker-sprint:latest .

REM if it runs on localhost
REM docker run -it -p 8080:8080 spring-general-worker-sprint:latest
REM if it runs on 192.168.1.100
REM docker run --env DATAPLATFORM_IP=192.168.1.100 -it -p 8080:8080 spring-general-worker-sprint:latest