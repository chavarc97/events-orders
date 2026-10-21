# Event-Driven Architecture

## Descripción General
- Simular el flujo señalado en verde del caso visto en clase.
- **Nota:** No es necesario implementar cobros ni enviar notificaciones; es suficiente con generar los eventos correspondientes.

---

## Servicios, Broker y Eventos

### Requisitos Técnicos
- **Aislamiento por contenedores:** Cada servicio y el broker deben ejecutarse en un contenedor separado.
- **Dockerización:**
  - Usar una imagen de Docker (`Dockerfile`).
  - Orquestar los servicios mediante Docker Compose (`docker-compose.yml` o `docker-compose.yaml`).
- **Message Broker:**
  - Opciones permitidas:
    - **RabbitMQ** (usando `pika` para Python) — *Recomendado si hay limitaciones de rendimiento o memoria en Docker.*
    - **Kafka** (usando `kafka-python`).
  - Utilizar imágenes oficiales de Docker para el broker elegido.

---

## Estructura de los Eventos
Los eventos deberán tener una estructura lógica en base a datos (ej. formato JSON):

- `event_id`: Identificador único del evento.
- `event_type`: Tipo de evento emitido.
- `timestamp`: Marca de tiempo de emisión.
- `producer`: Nombre del servicio que genera el evento.
- `payload`: Datos asociados al evento (datos de la orden).

---

## Evidencia y Trazabilidad del Flujo
- Generar evidencia dinámica sobre la ejecución del flujo (archivo de log, CSV, impresión en consola, etc.).
- El log se debe construir de manera dinámica al ejecutar la solución.
- Cada línea de registro debe incluir, por lo menos:
  ```text
  <service name> - <action (produced, consumed)> - <event_type>
  ```

---

## Ejecución del Flujo
El evento inicial (*initiating event*) deberá dispararse al correr el comando en consola:
```bash
docker compose run --rm app python place_order.py
```

---

## Entregables
- [ ] Código fuente y configuración de servicios.
- [ ] `Dockerfile` y `docker-compose.yaml`.
- [ ] `README.md` con instrucciones claras de configuración y ejecución.

## Order event flow

```mermaid
flowchart LR
    book["Book<br/>'Place an order for a book'"]

    ops[Order Placement<br/>service]
    ps[Payment<br/>service]
    ofs[Order Fulfillment<br/>service]
    ns[Notification<br/>service]

    e_placed(Order placed)
    e_applied(Payment applied)
    e_denied(Payment denied)
    e_fulfilled(Order fulfilled)
    e_email(Email sent)

    book --> ops
    ops -.-> e_placed
    e_placed -.-> ps
    ps -.-> e_applied
    ps -.-> e_denied
    e_applied -.-> ofs
    ofs -.-> e_fulfilled

    e_placed -.-> ns
    e_denied -.-> ns
    e_fulfilled -.-> ns
    ns <-.-> e_email

    classDef service fill:#dbeafe,stroke:#2563eb,color:#1e3a8a;
    classDef event fill:#ffffff,stroke:#6b7280,color:#111827;
    classDef actor fill:#f3f4f6,stroke:#6b7280,color:#111827;

    class ops,ps,ofs,ns service;
    class e_placed,e_applied,e_denied,e_fulfilled,e_email event;
    class book actor;
```

**Legend:** blue boxes are services, white rounded boxes are events, dotted arrows are event flow, and the solid arrow is the initial request.

``` mermaid
flowchart LR
    P[Producer] -->|"routing_key = 'order.placed'"| X{{"Exchange 'orders' (topic)"}}
    X -->|"bound to order.placed"| Q1[payment queue]
    X -->|"bound to order.placed"| Q2[notification queue]
    Q1 --> C1[Payment service]
    Q2 --> C2[Notification service]

```

