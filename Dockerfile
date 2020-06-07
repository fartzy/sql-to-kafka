FROM openjdk:8-jdk

RUN apt-get update && apt-get install -y iputils-ping net-tools

ADD . /opt/skyline
WORKDIR /opt/skyline

CMD ["/opt/skyline/skyline_runner.sh"]


# CMD ["java", "-cp", "/usr/local/app/target/JoinAction-1.0-standalone.jar:/usr/local/app/target/classes", "acme.JoinAction"]
