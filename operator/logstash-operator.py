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


@kopf.on.create('logstash-filter',param={'type':'filter','action':'create'})
@kopf.on.create('logstash-input',param={'type':'input','action':'create'})
@kopf.on.create('logstash-output',param={'type':'output','action':'create'})
@kopf.on.update('logstash-filter',param={'type':'filter','action':'update'})
@kopf.on.update('logstash-input',param={'type':'input','action':'update'})
@kopf.on.update('logstash-output',param={'type':'output','action':'update'})
@kopf.on.delete('logstash-filter',param={'type':'filter','action':'delete'})
@kopf.on.delete('logstash-input',param={'type':'input','action':'delete'})
@kopf.on.delete('logstash-output',param={'type':'output','action':'delete'})
def create_filter_fn(param,spec, name, namespace, logger, **kwargs):
    logger.info(f"{param['action']}: {name}")
 
    pipeline = spec.get('pipeline')
    configmapname = "logstash-operator-pipeline-"+pipeline
 
    api = kubernetes.client.CoreV1Api()
    if (param['action'] == 'create'):

        try: 
            api_response = api.read_namespaced_config_map(configmapname, namespace, pretty="true")
        except ApiException as e:
            if (e.status == 404):
                logger.info("Configmap not found, creating")
                create_configmap(configmapname,namespace)
            else:
                logger.error("Exception when calling CoreV1Api->read_namespaced_config_map: %s\n" % e)

    key = name+".conf"

    if (param['action'] == 'create' or param['action'] == 'update'):
        config_patch = {'data': {key: param['type']+'{\n'+ spec.get('data') + '\n}'}}
    if (param['action'] == 'delete'):
        config_patch = [{'op': 'remove','path': '/data/'+key }]

    obj = api.patch_namespaced_config_map(
        namespace=namespace,
        name=configmapname,
        body=config_patch,
    )

    logger.info(f"Configmap is updated: {obj.metadata.name}, {param['action']} {key}")
    return {'configmap-key': key,'configmap-name':configmapname}


