FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive TZ=Africa/Tunis
RUN apt-get update && apt-get install -y --no-install-recommends nginx openssh-server rsyslog tzdata \
 && rm -rf /var/lib/apt/lists/* \
 && sed -i '/imklog/s/^/#/' /etc/rsyslog.conf \
 && mkdir -p /run/sshd && ssh-keygen -A \
 && echo "<h1>web01 (conteneur) - Laboratoire ISET</h1>" > /var/www/html/index.html
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
EXPOSE 80 22
CMD ["/entrypoint.sh"]
