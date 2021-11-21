from alpine

RUN apk add --no-cache tini qpdf python3

COPY merge.py /opt/merge.py

ENTRYPOINT ["tini", "--"]
CMD ["/opt/merge.py"]
