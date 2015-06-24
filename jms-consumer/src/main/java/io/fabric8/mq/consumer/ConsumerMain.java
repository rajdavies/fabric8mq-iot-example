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
package io.fabric8.mq.consumer;

import io.fabric8.kubernetes.api.KubernetesClient;
import io.fabric8.kubernetes.api.model.ReplicationController;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.dataset.SimpleDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.AccessController;
import java.security.PrivilegedAction;

public class ConsumerMain {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerMain.class);
    public static void main(String args[]) {
        final int CONSUMER_COUNT = 5;
        try {
            String queueName = System.getenv("AMQ_QUEUENAME");

            if (queueName == null) {
                queueName = "IOTTEST";
            }

            final String QUEUE_NAME = queueName;

            String replicationId = "jms-consumer";

            KubernetesClient kubernetes = new KubernetesClient();
            ReplicationController replicationController = kubernetes.getReplicationController(replicationId);


            final int start = replicationController.getStatus().getReplicas();



            // create a camel route to consume messages from our queue
            org.apache.camel.main.Main main = new org.apache.camel.main.Main();
            main.bind("myDataSet", new SimpleDataSet());
            main.enableHangupSupport();

            main.addRouteBuilder(new RouteBuilder() {
                public void configure() {
                    for (int i =0; i < CONSUMER_COUNT; i++) {
                        final String destinationName = QUEUE_NAME + "." + (start+i);
                        from("amq:" + destinationName).to("dataset:myDataSet?retainLast=10");
                        System.err.println("Consuming on " + destinationName);
                    }
                }

            });

            main.run(args);

        } catch (Throwable e) {
            LOG.error("Failed to connect to Fabric8 MQ", e);
        }
    }
}
