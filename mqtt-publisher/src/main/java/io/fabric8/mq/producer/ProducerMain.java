/**
 *  Copyright 2005-2015 Red Hat, Inc.
 *
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package io.fabric8.mq.producer;

import io.fabric8.kubernetes.api.KubernetesClient;
import io.fabric8.kubernetes.api.model.ReplicationController;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ProducerMain {
    private static final Logger LOG = LoggerFactory.getLogger(ProducerMain.class);

    public static void main(String args[]) {
        try {
            final int PRODUCER_COUNT = 5;
            final String TOPIC = System.getenv("MQTT_TOPIC");
            final String host  = System.getenv("FABRIC8MQ_SERVICE_HOST");
            final String portStr = System.getenv("FABRIC8MQ_SERVICE_PORT");

            System.err.println("HOST = " + host);
            System.err.println("PORT = " + portStr);

            System.err.println("Connecting to " + host + ":" + portStr);

            final int port = Integer.valueOf(portStr.trim());

            //get the number of pods running for this

            String replicationId = "mqtt-producer";

            KubernetesClient kubernetes = new KubernetesClient();
            ReplicationController replicationController = kubernetes.getReplicationController(replicationId);

            int start = replicationController.getStatus().getReplicas();

            start = (start > 0) ? (start * PRODUCER_COUNT) : 0;
            ExecutorService executorService = Executors.newFixedThreadPool(PRODUCER_COUNT);

            for (int i =0; i < PRODUCER_COUNT; i++) {
                final String topic = TOPIC + "/" + (start + i);
                Runnable runnable = new Runnable() {
                    @Override
                    public void run() {
                        try {
                            System.err.println("Publishing on " + topic);

                            MQTT mqtt = new MQTT();
                            mqtt.setHost(host, port);
                            BlockingConnection mqttConnection = mqtt.blockingConnection();
                            mqttConnection.connect();

                            int i = 0;
                            while (true) {
                                String payload = "test:" + i++;
                                mqttConnection.publish(topic, payload.getBytes(), QoS.values()[1], false);
                                System.out.println("Published on " + topic + " " + payload);
                                Thread.sleep(1000);
                            }
                        }catch(Throwable e){
                            e.printStackTrace();
                        }
                    }
                };
                executorService.submit(runnable);
            }



        } catch (Throwable e) {
            LOG.warn("Failed to look up System properties for host and port", e);
        }
    }

    }
