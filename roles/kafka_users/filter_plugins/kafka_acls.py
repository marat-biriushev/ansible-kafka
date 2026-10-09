"""Filters for comparing kafka-acls.sh --list output with the desired ACLs."""

import re

DOCUMENTATION = r"""
  name: kafka_acls_parse
  short_description: Parse kafka-acls.sh --list output
  description:
    - Turns the text printed by kafka-acls.sh --list into a list of ACL bindings.
"""

RESOURCE_RE = re.compile(
    r"^Current ACLs for resource `ResourcePattern\(resourceType=(?P<type>[A-Z_]+), "
    r"name=(?P<name>.*), patternType=(?P<pattern>[A-Z]+)\)`:"
)
ENTRY_RE = re.compile(
    r"^\s*\(principal=(?P<principal>.+?), host=(?P<host>.+?), "
    r"operation=(?P<operation>[A-Z_]+), permissionType=(?P<permission>[A-Z]+)\)"
)


def _norm(value):
    """READ, Read, DESCRIBE_CONFIGS, DescribeConfigs -> read, describeconfigs."""
    return str(value).replace("_", "").replace("-", "").lower()


def _key(resource_type, name, pattern, principal, host, operation, permission):
    if _norm(resource_type) == "cluster":
        name = "kafka-cluster"
    return "|".join([
        _norm(resource_type), str(name), _norm(pattern), str(principal),
        str(host), _norm(operation), _norm(permission),
    ])


def kafka_acls_parse(text):
    """Return one key string per existing ACL binding."""
    keys = []
    resource = None
    for line in str(text).splitlines():
        match = RESOURCE_RE.match(line)
        if match:
            resource = match
            continue
        match = ENTRY_RE.match(line)
        if match and resource:
            keys.append(_key(
                resource.group("type"), resource.group("name"), resource.group("pattern"),
                match.group("principal"), match.group("host"),
                match.group("operation"), match.group("permission"),
            ))
    return keys


def kafka_acls_expand(acls):
    """Expand the role's ACL list (several operations per entry) into one dict
    per binding, each carrying the same key kafka_acls_parse produces."""
    result = []
    for acl in acls:
        resource_type = acl.get("resource_type", "topic")
        pattern = acl.get("pattern_type", "literal")
        host = acl.get("host", "*")
        permission = acl.get("permission", "allow")
        name = acl.get("name", "kafka-cluster")
        for operation in acl["operations"]:
            result.append({
                "principal": acl["principal"],
                "resource_type": resource_type,
                "name": name,
                "pattern_type": pattern,
                "host": host,
                "permission": permission,
                "operation": operation,
                "key": _key(resource_type, name, pattern, acl["principal"], host, operation, permission),
            })
    return result


class FilterModule:
    def filters(self):
        return {
            "kafka_acls_parse": kafka_acls_parse,
            "kafka_acls_expand": kafka_acls_expand,
        }
