FROM gradle:3.5-jdk8-alpine
ADD ./build /home/gradle/build/
ADD build.gradle /home/gradle
CMD ["gradle", "runConsumer"]