FROM hurence/spark:2.3.3

LABEL maintainer="support@hurence.com"


RUN apk add --update --no-cache \
      build-base \
      openblas-dev \
      unzip \
      wget \
      cmake \
      libjpeg  \
      libjpeg-turbo-dev \
      libpng-dev \
      jasper-dev \
      tiff-dev \
      libwebp-dev \
      clang-dev \
      linux-headers \
      python \
      py-pip \
      python-dev \
      apache-ant && \
    pip install numpy

ENV CC /usr/bin/clang
ENV CXX /usr/bin/clang++
ENV OPENCV_VERSION 4.1.1
ENV  JAVA_HOME /opt/jdk

RUN cd /opt && \
  wget https://github.com/opencv/opencv/archive/${OPENCV_VERSION}.zip && \
  unzip ${OPENCV_VERSION}.zip && \
  rm -rf ${OPENCV_VERSION}.zip

RUN mkdir -p /opt/opencv-${OPENCV_VERSION}/build && \
  cd /opt/opencv-${OPENCV_VERSION}/build && \
  cmake \
  -D CMAKE_BUILD_TYPE=RELEASE \
  -D CMAKE_INSTALL_PREFIX=/usr/local \
  -D PYTHON2_EXECUTABLE=/usr/bin/python \
  -D PYTHON_INCLUDE_DIR=/usr/include/python2.7  \
  -D WITH_FFMPEG=NO \
  -D WITH_IPP=NO \
  -D WITH_OPENEXR=NO \
  -D WITH_TBB=NO \
  -D BUILD_EXAMPLES=NO \
  -D BUILD_ANDROID_EXAMPLES=NO \
  -D INSTALL_PYTHON_EXAMPLES=NO \
  -D BUILD_DOCS=NO \
  -D BUILD_opencv_python2=NO \
  -D BUILD_opencv_python3=NO \
  -D BUILD_opencv_java=ON \
  -D BUILD_SHARED_LIBS=OFF \
  -D BUILD_EXAMPLES=OFF \
  -D BUILD_TESTS=OFF \
  -D BUILD_PERF_TESTS=OFF \
  .. && \
  make -j8 && \
  make install

COPY ./logisland-*.tar.gz /tmp/
RUN cd /tmp; \
        tar -xzf logisland-*.tar.gz; \
        rm -f /tmp/*.gz; \
        mv logisland-* /opt
RUN cd /opt && ln -s $(eval ls | grep logisland) logisland
ENV LOGISLAND_HOME /opt/logisland
ENV PATH $PATH:$LOGISLAND_HOME/bin
WORKDIR $LOGISLAND_HOME/

# If you set this entry point integration test can not execute properly (never ending...)
#ENTRYPOINT ["tail", "-f", "bin/logisland.sh"]