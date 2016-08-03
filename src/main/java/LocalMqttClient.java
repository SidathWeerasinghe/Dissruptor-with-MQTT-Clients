/*
 * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;

import java.io.File;

/**
 * This sample demonstrates how to write a simple MQTT client to
 * send/receive message via MQTT in WSO2 Message Broker.
 */
class LocalMqttClient {

    private static String brokerURL;
    private String topic;
    String publisherClientId;
    private org.eclipse.paho.client.mqttv3.MqttClient mqttPublisherClient;

    /**
     * Making a MQTT client that passes messages between Disruptor to message broker.
     * @param brokerURL Define the broker address with it's port number. Eg: tcp://localhost:1883
     * @param topic Define the Topic Name that displaying.
     * @param publisherClientId Define the unique number that given to each of the Client.
     */
    LocalMqttClient(String brokerURL, String topic, String publisherClientId) {

        this.brokerURL = brokerURL;
        this.topic = topic;
        this.publisherClientId = publisherClientId;

        // Displaying a Broker URL when broker starts.
        log.info("Running Client URL " + brokerURL);

        try {
            // Creating mqtt publisher client
            mqttPublisherClient = getNewMqttClient(publisherClientId);
        } catch (MqttException e) {
            log.error("Error generating new mqtt Client" , e);
        }
    }

    private static final Log log = LogFactory.getLog(MqttClient.class);
    /**
     * Java temporary directory location that store messages until
     * Message broker fetches them.
     */
    private static final String JAVA_TMP_DIR = System.getProperty("java.io.tmpdir");

    /**
     * Publishing messages to mqtt topic "simpleTopic".
     * @param message Message that gets form the disruptor.
     * @throws MqttException when publishing the message.
     */
    void publishMessage(byte[] message) throws MqttException {
         mqttPublisherClient.publish(topic, message, QualityOfService.LEAST_ONCE.getValue(), false);
    }

    /**
     * Crate a new MQTT client and connect it to the server.
     * @param clientId The unique mqtt client Id.
     * @return Connected MQTT client.
     * @throws MqttException when generating a new MQTT client.
     */
    private static org.eclipse.paho.client.mqttv3.MqttClient getNewMqttClient(String clientId) throws MqttException {
        //Store messages until server fetches them.
        MqttDefaultFilePersistence dataStore = new MqttDefaultFilePersistence(
                JAVA_TMP_DIR + File.separator + clientId);

        org.eclipse.paho.client.mqttv3.MqttClient mqttClient = new MqttClient(brokerURL, clientId, dataStore);
        SimpleMQTTCallback callback = new SimpleMQTTCallback();
        mqttClient.setCallback(callback);

        MqttConnectOptions connectOptions = new MqttConnectOptions();

        connectOptions.setUserName("admin");
        connectOptions.setPassword("admin".toCharArray());
        connectOptions.setCleanSession(true);
        mqttClient.connect(connectOptions);
        return mqttClient;
    }

}