FROM apache/hive:3.1.3

USER root
ADD https://jdbc.postgresql.org/download/postgresql-42.7.5.jar /opt/hive/lib/postgres.jar
RUN chmod 644 /opt/hive/lib/postgres.jar && rm -f /opt/hive/lib/postgresql-9.4.1208.jre7.jar
USER hive