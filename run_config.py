import xml.etree.ElementTree as et
import tube.settings as config


CONFIG_PATH = '{}/etc/hadoop/'.format(config.HADOOP_HOME)


def configure_core_site():
    core_site_path = '{}core-site.xml'.format(CONFIG_PATH)
    tree = et.parse(core_site_path)
    root = tree.getroot()
    root.append(create_property('hadoop.tmp.dir', '{}/hdfs/tmp'.format(config.HADOOP_HOME)))
    root.append(create_property('fs.default.name', config.HADOOP_URL))
    tree.write(core_site_path)


def configure_hdfs_site():
    core_site_path = '{}hdfs-site.xml'.format(CONFIG_PATH)
    tree = et.parse(core_site_path)
    root = tree.getroot()
    root.append(create_property('dfs.replication', '1'))
    tree.write(core_site_path)


def configure_mapred_site():
    core_site_path = '{}mapred-site.xml'.format(CONFIG_PATH)
    tree = et.parse(core_site_path)
    root = tree.getroot()
    root.append(create_property('mapreduce.framework.name', 'yarn'))
    root.append(create_property('mapreduce.application.classpath', '$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*'))
    tree.write(core_site_path)


def configure_yarn_site():
    core_site_path = '{}yarn-site.xml'.format(CONFIG_PATH)
    tree = et.parse(core_site_path)
    root = tree.getroot()
    root.append(create_property('yarn.nodemanager.aux-services', 'mapreduce_shuffle'))
    root.append(create_property('yarn.resourcemanager.scheduler.address', '{}:8030'.format(config.HADOOP_HOST)))
    root.append(create_property('yarn.resourcemanager.resource-tracker.address', '{}:8031'.format(config.HADOOP_HOST)))
    root.append(create_property('yarn.resourcemanager.address', '{}:8032'.format(config.HADOOP_HOST)))
    tree.write(core_site_path)


def create_property(prop_name, prop_val):
    prop = et.Element('property')
    name = et.Element('name')
    name.text = prop_name
    value = et.Element('value')
    value.text = prop_val
    prop.append(name)
    prop.append(value)
    return prop


if __name__ == '__main__':
    # Execute Main functionality
    configure_core_site()
    configure_hdfs_site()
    configure_mapred_site()
    configure_yarn_site()
