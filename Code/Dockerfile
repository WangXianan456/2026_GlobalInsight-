FROM flink:1.17.1-java8

# 将 Jar 包放入 Flink 的标准用户库目录
# 这样 Flink 启动时会自动识别，且提交任务时无需上传
COPY target/GlobalInsight-1.0.jar /opt/flink/usrlib/GlobalInsight.jar

# 并在 /app 目录下创建一个软链接，以便兼容 MockDataGenerator 的启动命令
RUN mkdir -p /app && ln -s /opt/flink/usrlib/GlobalInsight.jar /app/GlobalInsight.jar

