# QuestDB Kubernetes Liveness and Readiness Probes

This guide shows an example of how to configure **liveness** and **readiness** probes when deploying **QuestDB** with Kubernetes.

---

## 📄 Example Deployment YAML

Below is a **working example** you can adapt for your cluster:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: questdb
spec:
  replicas: 1
  selector:
    matchLabels:
      app: questdb
  template:
    metadata:
      labels:
        app: questdb
    spec:
      containers:
        - name: questdb
          image: questdb/questdb:latest
          ports:
            - containerPort: 9000
          livenessProbe:
            httpGet:
              path: /
              port: 9000
            initialDelaySeconds: 30
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /
              port: 9000
            initialDelaySeconds: 5
            periodSeconds: 10
