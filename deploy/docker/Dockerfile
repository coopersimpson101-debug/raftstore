FROM python:3.12-slim
WORKDIR /opt/raftstore
COPY raftstore ./raftstore
ENV PYTHONUNBUFFERED=1
EXPOSE 7000 8080
ENTRYPOINT ["python", "-m", "raftstore"]
CMD ["serve", "--config", "/etc/raftstore/node.json"]
