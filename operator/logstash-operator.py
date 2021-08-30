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
    kopf.adopt(data)

    api = kubernetes.client.CoreV1Api()
        
    obj = api.create_namespaced_config_map(
        namespace=namespace,
        body=data,
    )
    return obj

@kopf.on.create('logstash-deployment',param={'type':'deployment','action':'create'})
@kopf.on.update('logstash-deployment',param={'type':'deployment','action':'update'})
def create_statefulset_fn(param, spec, name, namespace, logger, **kwargs):
    logger.info(f"Creating: {name}")

    replicas = spec.get('replicas')
    if not replicas:
        raise kopf.PermanentError(f"Replicas must be set. Got {replicas!r}.")
    pipelines = spec.get('pipelines')
    logstashconfig = spec.get('config')
    image = spec.get('image')

    configconfigmapname = name+"-logstash-operator-config"
    pipelinesconfigmapname = name+"-logstash-operator-pipelines"

    templateLoader = jinja2.FileSystemLoader(searchpath="./templates/")
    templateEnv = jinja2.Environment(loader=templateLoader)

    ## render configmap for logstash config
    TEMPLATE_FILE = "configmap-config.yaml.j2"
    template = templateEnv.get_template(TEMPLATE_FILE)
    text = template.render(name=configconfigmapname,config=logstashconfig)
    configdata = yaml.safe_load(text)

    ## Render configmap for logstash pipelines config locations
    TEMPLATE_FILE = "configmap-pipelines.yaml.j2"
    template = templateEnv.get_template(TEMPLATE_FILE)
    text = template.render(name=pipelinesconfigmapname,pipelines=pipelines)
    pipelinesdata = yaml.safe_load(text)

    ## Get all inputs for this statefulset and create service for each
    services={}
    ports={}
    customobjectsapi = kubernetes.client.CustomObjectsApi()
    customobjectsapi_response = customobjectsapi.list_namespaced_custom_object("logstash-operator.qalo.de","v1",namespace,"logstash-inputs", pretty="true", label_selector=spec.get('selector'))
    TEMPLATE_FILE = "service.yaml.j2"
    template = templateEnv.get_template(TEMPLATE_FILE)
    for item in customobjectsapi_response['items']:
        ports[item['metadata']['name']]=item['spec']['port']
        servicename=name+"-"+item['metadata']['name']
        text = template.render(name=servicename,app=name,port=item['spec']['port'],portname=item['metadata']['name'])
        servicedata = yaml.safe_load(text)
        services[servicename]=servicedata

    ## Render statefulset manifest
    TEMPLATE_FILE = "statefulset.yaml.j2"
    template = templateEnv.get_template(TEMPLATE_FILE)
    text = template.render(name=name, replicas=replicas, pipelines=pipelines, image=image, ports=ports)
    statefulsetdata = yaml.safe_load(text)

    # Create
    if param['action'] == 'create':
        ## Create configmap for logstash config
        kopf.adopt(configdata)
        api = kubernetes.client.CoreV1Api()
        obj = api.create_namespaced_config_map(
            namespace=namespace,
            body=configdata,
        )
        logger.info(f"Created configmap: {obj.metadata.name}")

        ## Create configmap for logstash pipelines config locations
        kopf.adopt(pipelinesdata)
        api = kubernetes.client.CoreV1Api()
        obj = api.create_namespaced_config_map(
            namespace=namespace,
            body=pipelinesdata,
        )
        logger.info(f"Created configmap: {obj.metadata.name}")

        ## Crete Statefulset
        kopf.adopt(statefulsetdata)
        api = kubernetes.client.AppsV1Api()
        obj = api.create_namespaced_stateful_set(
            namespace=namespace,
            body=statefulsetdata,
        )
        logger.info(f"Created Statefulset: {obj.metadata.name}")


    ## Update 
    if param['action'] == 'update':

        ## Update configmap for logstash config
        kopf.adopt(configdata)
        api = kubernetes.client.CoreV1Api()
        obj = api.patch_namespaced_config_map(
            namespace=namespace,
            name = configconfigmapname,
            body=configdata,
        )
    
        ## Update configmap for logstash pipelines config locations
        kopf.adopt(pipelinesdata)
        api = kubernetes.client.CoreV1Api()
        obj = api.patch_namespaced_config_map(
            namespace=namespace,
            name = pipelinesconfigmapname,
            body=pipelinesdata,
        )
        logger.info(f"Updated pipeline configmap: {obj.metadata.name}")

        # Update statefulset
        api = kubernetes.client.AppsV1Api()
        obj = api.patch_namespaced_stateful_set(
            namespace=namespace,
            name=name,
            body=statefulsetdata,
        )
        logger.info(f"Updated Statefulset: {obj.metadata.name}")

    # Update or create services
    for servicename,servicedata in services.items():
        api = kubernetes.client.CoreV1Api()
        try: 
            api_response = api.read_namespaced_service(servicename, namespace, pretty="true")
        except ApiException as e:
            if (e.status == 404):
                kopf.adopt(servicedata)
                obj = api.create_namespaced_service(
                    namespace=namespace,
                    body=servicedata,
                )
                logger.info(f"Created service: {obj.metadata.name}")
        else:
            print(servicedata)
            obj = api.replace_namespaced_service (
                namespace=namespace,
                name = name+"-"+item['metadata']['name'],
                body=servicedata,
            )
            logger.info(f"Updated service: {obj.metadata.name}")


@kopf.on.create('logstash-pipeline',param={'type':'pipeline','action':'create'})
@kopf.on.update('logstash-pipeline',param={'type':'pipeline','action':'update'})
def create_pipeline_fn(param,spec, name, namespace, logger, **kwargs):
    logger.info(f"{param['action']}: {name}")
    configmapname=name
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
    
    coreapi = kubernetes.client.CoreV1Api()    
    customobjectsapi = kubernetes.client.CustomObjectsApi()

    customobjectsapi_response = customobjectsapi.list_namespaced_custom_object("logstash-operator.qalo.de","v1",namespace,"logstash-filters", pretty="true", label_selector=spec.get('selector'))
    for item in customobjectsapi_response['items']:
        key = str(item['spec']['order'])+'-'+item['metadata']['name']+".conf"

        if (param['action'] == 'create' or param['action'] == 'update'):
            config_patch = {'data': {key: 'filter {\n'+ item['spec']['data'] + '\n}'}}

        obj = coreapi.patch_namespaced_config_map(
            namespace=namespace,
            name=configmapname,
            body=config_patch,
        )

        logger.info(f"Updated: {obj.metadata.name}, {param['action']} {key}")

    customobjectsapi_response = customobjectsapi.list_namespaced_custom_object("logstash-operator.qalo.de","v1",namespace,"logstash-inputs", pretty="true", label_selector=spec.get('selector'))
    key = "input.conf"
    t = jinja2.Template("input {\n {% for item in items %}{{ item.spec.data }}{% endfor %} \n}")
    data = t.render(items=customobjectsapi_response['items'])
    patch = {'data': {key: data}}

    obj = coreapi.patch_namespaced_config_map(
        namespace=namespace,
        name=configmapname,
        body=patch,
    )
    logger.info(f"Updated: {obj.metadata.name}, {param['action']} {key}")

    customobjectsapi_response = customobjectsapi.list_namespaced_custom_object("logstash-operator.qalo.de","v1",namespace,"logstash-outputs", pretty="true", label_selector=spec.get('selector'))
    key = "output.conf"
    t = jinja2.Template("output {\n {% for item in items %}{{ item.spec.data }}{% endfor %} \n}")
    data = t.render(items=customobjectsapi_response['items'])
    patch = {'data': {key: data}}

    obj = coreapi.patch_namespaced_config_map(
        namespace=namespace,
        name=configmapname,
        body=patch,
    )
    logger.info(f"Updated: {obj.metadata.name}, {param['action']} {key}")

@kopf.on.create('logstash-filter',param={'type':'filter','action':'create'})
@kopf.on.create('logstash-input',param={'type':'input','action':'create'})
@kopf.on.create('logstash-output',param={'type':'output','action':'create'})
@kopf.on.update('logstash-filter',param={'type':'filter','action':'update'})
@kopf.on.update('logstash-input',param={'type':'input','action':'update'})
@kopf.on.update('logstash-output',param={'type':'output','action':'update'})
@kopf.on.delete('logstash-filter',param={'type':'filter','action':'delete'})
@kopf.on.delete('logstash-input',param={'type':'input','action':'delete'})
@kopf.on.delete('logstash-output',param={'type':'output','action':'delete'})
def pipelineelement_fn(param,spec, name, namespace, logger, **kwargs):
    logger.info(f"{param['action']}: {name}")
 
    customobjectsapi = kubernetes.client.CustomObjectsApi()
    customobjectsapi_response = customobjectsapi.list_namespaced_custom_object("logstash-operator.qalo.de","v1",namespace,"logstash-pipelines", pretty="true")
    for item in customobjectsapi_response['items']:
        patchversion = int(item['metadata'].get('annotations',{}).get('logstash-operator.qalo.de/patchversion',"0"))+1
        patch = {'metadata':{'annotations':{'logstash-operator.qalo.de/patchversion' : str(patchversion)}}}
        logger.info(f"Trigger update on pipeline {item['metadata']['name']}, patchversion {patchversion}")

        obj = customobjectsapi.patch_namespaced_custom_object("logstash-operator.qalo.de","v1",namespace,"logstash-pipelines",
            name=item['metadata']['name'],
            body=patch,
        )
    if param['type'] == 'input':
        customobjectsapi_response = customobjectsapi.list_namespaced_custom_object("logstash-operator.qalo.de","v1",namespace,"logstash-deployments", pretty="true")
        for item in customobjectsapi_response['items']:
            patchversion = int(item['metadata'].get('annotations',{}).get('logstash-operator.qalo.de/patchversion',"0"))+1
            patch = {'metadata':{'annotations':{'logstash-operator.qalo.de/patchversion' : str(patchversion)}}}
            logger.info(f"Trigger update on deployment {item['metadata']['name']}, patchversion {patchversion}")

            obj = customobjectsapi.patch_namespaced_custom_object("logstash-operator.qalo.de","v1",namespace,"logstash-deployments",
                name=item['metadata']['name'],
                body=patch,
            )
