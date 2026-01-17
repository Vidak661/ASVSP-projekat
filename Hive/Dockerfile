FROM bde2020/hive:2.3.2-postgresql-metastore

RUN printf "deb http://archive.debian.org/debian stretch main\n" > /etc/apt/sources.list && \
    printf 'Acquire::Check-Valid-Until "false";\nAcquire::AllowInsecureRepositories "true";\nAcquire::AllowDowngradeToInsecureRepositories "true";\n' > /etc/apt/apt.conf.d/99insecure && \
    apt-get -o Acquire::Check-Valid-Until=false update && \
    apt-get install -y --allow-unauthenticated openssh-server && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /var/run/sshd

RUN sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
RUN sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config
RUN echo '# Hadoop config\nexport HADOOP_HOME=/opt/hadoop-2.7.4' >> /opt/hive/bin/hive-config.sh \
    && echo 'export HADOOP_PREFIX=/opt/hadoop-2.7.4' >> /opt/hive/bin/hive-config.sh \
    && echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/' >> /opt/hive/bin/hive-config.sh
RUN echo "root:asvsp" | chpasswd

EXPOSE 22
