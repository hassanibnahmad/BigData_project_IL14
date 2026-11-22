# Shared IL-14 Kafka on a VPS

Run this Kafka-only Compose project beside your website projects. The broker publishes TCP 39092 on the VPS's Tailscale IPv4 address. Nginx continues to handle the websites on their existing ports. The broker's internal listener and controller stay on the Compose network. [Kafka listeners](https://kafka.apache.org/39/configuration/broker-configs/) · [Docker port binding](https://docs.docker.com/engine/network/port-publishing/)

## 1. Check the VPS

Connect to the VPS over SSH. Check its available RAM, disk, running containers, and whether the Kafka port is already in use. Allow headroom for your existing websites as well as Kafka. The broker has a 1 GiB container memory limit and a 512 MiB JVM heap limit.

```bash
docker compose version
docker ps --format 'table {{.Names}}\t{{.Ports}}'
free -h
df -h / "$(docker info --format '{{.DockerRootDir}}')"
ss -lnt 'sport = :39092'
```

Join the VPS and the machines or runtimes of A, B, and C to a Tailscale network. Set its access policy so the team can reach TCP 39092 on the VPS. Check that `tailscale ip -4` on the VPS gives its private address. Keep the broker's port off the public IP and leave the website firewall rules in place. [Tailscale connectivity](https://tailscale.com/docs/how-to/connect-to-devices)

## 2. Copy just the Compose file

From your computer, at the repository root, replace `YOUR_SSH_USER@YOUR_VPS_HOST` with your usual SSH login. Use a dedicated directory so the Kafka project and its volume stay separate from your website projects.

```bash
ssh YOUR_SSH_USER@YOUR_VPS_HOST 'mkdir -p ~/il14-kafka'
scp deploy/kafka-vps/compose.yaml YOUR_SSH_USER@YOUR_VPS_HOST:~/il14-kafka/compose.yaml
```

If this file has already been pushed to a branch available on the VPS, a pull followed by `cd deploy/kafka-vps` also works. Use that directory for all remaining Compose commands.

## 3. Initialize and start Kafka

On the VPS, run these commands in the dedicated directory (`~/il14-kafka` if you copied the file). The first run generates a cluster ID. Keep the same `.env` and named volume for subsequent runs.

```bash
cd ~/il14-kafka
docker pull apache/kafka:3.9.1
if [ ! -e .env ]; then
  (
    set -eu
    ip=$(tailscale ip -4)
    test -n "$ip"
    cluster_id=$(docker run --rm --entrypoint /opt/kafka/bin/kafka-storage.sh \
      apache/kafka:3.9.1 random-uuid)
    test -n "$cluster_id"
    umask 077
    printf 'KAFKA_TAILSCALE_IP=%s\nKAFKA_CLUSTER_ID=%s\n' "$ip" "$cluster_id" > .env
  )
fi
docker compose config --quiet && docker compose up -d
docker compose ps
docker compose logs --tail=60 kafka
```

Confirm the published port in `docker compose ps` uses the VPS Tailscale IP. If Kafka starts before Tailscale is ready after a VPS reboot, check `tailscale ip -4` and run `docker compose up -d` again.

## 4. Create and verify the topics

Run this from the same directory once the broker is ready. Kafka must list topics successfully before provisioning. If startup is still in progress, check the logs and retry.

```bash
(
  set -e
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:29092 --list
  for topic in positions_vehicules predictions_arrets horaires_theoriques; do
    docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server kafka:29092 --create --if-not-exists \
      --topic "$topic" --partitions 3 --replication-factor 1
  done
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:29092 --describe
)
```

Create `stop_events_v1` with the same partition and replication settings if the team is ready to publish simulated arrivals.

## 5. Hand off the endpoint

Send A, B, and C the value from `tailscale ip -4` with `:39092` appended. Each sets `KAFKA_BOOTSTRAP_SERVERS=<VPS-Tailscale-IP>:39092` in the environment of the actual process or container. A points the position and stop-time producers to the new broker. B points Bronze ingestion there. C uses it for the `positions` and `mbta` Spark sources. [Spark launch commands](../../spark/README.md#live-mbta-input-from-student-a)

Start with a fresh checkpoint and storage generation for the new broker. Ask A to publish, then have B and C each consume a real record from their own runtime. An open port alone does not confirm that Kafka's advertised listener is usable. [Kafka advertised listeners](https://kafka.apache.org/39/configuration/broker-configs/)

## Keep the broker running

Use `docker compose ps`, `docker compose logs --tail=100 kafka`, and `docker stats --no-stream "$(docker compose ps -q kafka)"` to check it. Run `docker compose stop kafka` to stop just the broker. Keep the named volume when stopping or upgrading it.

The defaults retain up to seven days or 256 MiB per partition, whichever is reached first. This is a retention target, and active segments, indexes, and other Kafka files need extra disk headroom. Adjust `KAFKA_RETENTION_BYTES` in `.env` if Spark or Bronze needs a longer replay window and the disk has room. [Kafka retention settings](https://kafka.apache.org/39/configuration/topic-level-configs/)
