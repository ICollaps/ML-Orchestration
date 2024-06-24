FROM continuumio/miniconda3

RUN conda install -c conda-forge mlflow
EXPOSE 5000

CMD mlflow server --backend-store-uri sqlite:///mlflow.db --default-artifact-root /mlflow/mlruns --host 0.0.0.0 --port 5000
