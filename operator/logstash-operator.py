import os
import kopf
import kubernetes
import yaml
import jinja2
from kubernetes.client.rest import ApiException


def create_configmap(name,namespace):
    templateLoader = jinja2.FileSystemLoader(searchpath="./templates/")
    templateEnv = jinja2.Environment(loader=templateLoader)
    TEMPLATE_FILE = "configmap.yaml.j2"
    template = templateEnv.get_template(TEMPLATE_FILE)
    text = template.render(name=name)
    data = yaml.safe_load(text)

    api = kubernetes.client.CoreV1Api()
        
    obj = api.create_namespaced_config_map(
        namespace=namespace,
        body=data,
    )
    return obj

@kopf.on.create('logstash-deployment')
def create_statefulset_fn(spec, name, namespace, logger, **kwargs):
    logger.info(f"Creating: {name}")

    replicas = spec.get('replicas')
    if not replicas:
        raise kopf.PermanentError(f"Replicas must be set. Got {replicas!r}.")
    pipelines = spec.get('pipelines')
    logstashconfig = spec.get('config')

    templateLoader = jinja2.FileSystemLoader(searchpath="./templates/")
    templateEnv = jinja2.Environment(loader=templateLoader)

    TEMPLATE_FILE = "configmap-config.yaml.j2"
    template = templateEnv.get_template(TEMPLATE_FILE)
    text = template.render(name=name,config=logstashconfig)
    data = yaml.safe_load(text)
    kopf.adopt(data)
    api = kubernetes.client.CoreV1Api()
    obj = api.create_namespaced_config_map(
        namespace=namespace,
        body=data,
    )

    TEMPLATE_FILE = "configmap-pipelines.yaml.j2"
    template = templateEnv.get_template(TEMPLATE_FILE)
    text = template.render(name=name,pipelines=pipelines)
    data = yaml.safe_load(text)
    kopf.adopt(data)
    api = kubernetes.client.CoreV1Api()
    obj = api.create_namespaced_config_map(
        namespace=namespace,
        body=data,
    )


    TEMPLATE_FILE = "statefulset.yaml.j2"
    template = templateEnv.get_template(TEMPLATE_FILE)
    text = template.render(name=name, replicas=replicas, pipelines=pipelines)
    data = yaml.safe_load(text)
    kopf.adopt(data)
    api = kubernetes.client.AppsV1Api()
    obj = api.create_namespaced_stateful_set(
        namespace=namespace,
        body=data,
    )
    logger.info(f"Statefulset child is created: {obj.metadata.name}")
    return {'objname': obj.metadata.name}

@kopf.on.update('logstash-deployment')
def update_statefulset_fn(spec, status, namespace, logger, **kwargs):

    replicas = spec.get('replicas')
    if not replicas:
        raise kopf.PermanentError(f"Replicas must be set. Got {replicas!r}.")

    statefulset_name = status['create_statefulset_fn']['objname']
    statefulset_patch = {'spec': {'replicas': replicas}}

    logger.info(f"Updating: {statefulset_name}")
    
    api = kubernetes.client.AppsV1Api()
    obj = api.patch_namespaced_stateful_set(
        namespace=namespace,
        name=statefulset_name,
        body=statefulset_patch,
    )

    logger.info(f"Statefulset child is updated: {obj.metadata.name}")


@kopf.on.create('logstash-filter')
def create_filter_fn(spec, name, namespace, logger, **kwargs):
    logger.info(f"Creating: {name}")

    filter = spec.get('filter')
    pipeline = spec.get('pipeline')
    configmapname = "logstash-operator-pipeline-"+pipeline
    if not filter:
        raise kopf.PermanentError(f"filter must be set. Got {filter!r}.")
    if not pipeline:
        raise kopf.PermanentError(f"pipeline must be set. Got {pipeline!r}.")

    api = kubernetes.client.CoreV1Api()

    try: 
        api_response = api.read_namespaced_config_map(configmapname, namespace, pretty="true")
    except ApiException as e:
        if (e.status == 404):
            logger.info("Configmap not found, creating")
            create_configmap(configmapname,namespace)
        else:
            logger.error("Exception when calling CoreV1Api->read_namespaced_config_map: %s\n" % e)

    key = name+".conf"
    config_patch = {'data': {key: filter}}

    obj = api.patch_namespaced_config_map(
        namespace=namespace,
        name=configmapname,
        body=config_patch,
    )

    logger.info(f"Configmap is updated: {obj.metadata.name}, created {key}")
    return {'configmap-key': key,'configmap-name':configmapname}

@kopf.on.update('logstash-filter')
def update_filter_fn(spec, status, namespace, logger, **kwargs):
    filter = spec.get('filter')
    pipeline = spec.get('pipeline')
    if not filter:
        raise kopf.PermanentError(f"filter must be set. Got {filter!r}.")
    if not pipeline:
        raise kopf.PermanentError(f"pipeline must be set. Got {pipeline!r}.")

    api = kubernetes.client.CoreV1Api()

    key = status['create_filter_fn']['configmap-key']
    configmapname = status['create_filter_fn']['configmap-name']
    config_patch = {'data': { key: filter}}

    obj = api.patch_namespaced_config_map(
        namespace=namespace,
        name=configmapname,
        body=config_patch,
    )

    logger.info(f"Configmap is updated: {obj.metadata.name}, {key}")


@kopf.on.delete('logstash-filter')
def delete_filter_fn(status, namespace, logger, **kwargs):

    key = status['create_filter_fn']['configmap-key']
    configmapname = status['create_filter_fn']['configmap-name']
    config_patch = [{'op': 'remove','path': '/data/'+key }]
    logger.info(f"Deleting: {key}")

    api = kubernetes.client.CoreV1Api()
    obj = api.patch_namespaced_config_map(
        namespace=namespace,
        name=configmapname,
        body=config_patch,
    )

    logger.info(f"Configmap is updated: {obj.metadata.name}, deleted {key}")
