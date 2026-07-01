ARG BEAM_VERSION=2.74.0
FROM nvidia/cuda:12.9.1-cudnn-runtime-ubuntu24.04 AS cuda

FROM apache/beam_java21_sdk:${BEAM_VERSION}

# Copy CUDA runtime libraries from NVIDIA image
COPY --from=cuda /usr/local/cuda/lib64/ /usr/local/cuda/lib64/
COPY --from=cuda /usr/lib/x86_64-linux-gnu/libcudnn* /usr/lib/x86_64-linux-gnu/

ENV LD_LIBRARY_PATH="/usr/local/cuda/lib64:${LD_LIBRARY_PATH}"
ENV NVIDIA_VISIBLE_DEVICES=all
ENV NVIDIA_DRIVER_CAPABILITIES=compute,utility
