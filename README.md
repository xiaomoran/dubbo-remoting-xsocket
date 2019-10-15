# dubbo-remoting-xsocket

A transporter Extension for xSocket(http://www.xsocket.org/)

## Maven dependency：
```xml

        <dependency>
            <groupId>org.xsocket</groupId>
            <artifactId>xSocket</artifactId>
            <version>2.8.15</version>
        </dependency>

```

## Configure：
Define  protocol:
```
 <dubbo:protocol  server="xSocket" />
```

```
<dubbo:reference  client="xSocket"  />
```
