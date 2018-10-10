# lib-rserver

Accesible desde el [nexus interno](https://gitlab.predictia.es/snippets/9)

```xml
<dependency>
  <groupId>es.predictia</groupId>
  <artifactId>rserver</artifactId>
  <version>0.0.1-SNAPSHOT</version>
</dependency>
```

## Desplegar snapshot y site

```bash
cd /tmp && git clone https://gitlab.predictia.es/lib/rserver.git lib-rserver && cd lib-rserver
mvn deploy site site:deploy
```# rserver
