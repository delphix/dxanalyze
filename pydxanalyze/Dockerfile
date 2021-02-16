FROM python:3-slim

WORKDIR /pydxhealthcheck

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
RUN mkdir dxanalyze && mkdir delphixpy && mkdir /process && mkdir /output

COPY pydxhealthcheck.py .
COPY dxanalyze/ dxanalyze/
COPY delphixpy/ delphixpy/
COPY pydxanalyze-template.pptx .
COPY pydxfarmanalyze-template.pptx .

RUN chmod +x ./pydxhealthcheck.py
RUN ln -s ./pydxhealthcheck.py pydxfarmanalyze
RUN ln -s ./pydxhealthcheck.py pydxhealthcheck

ENV PATH $PATH:/pydxhealthcheck:/pydxfarmanalyze
