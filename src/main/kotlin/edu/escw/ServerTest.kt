package edu.escw

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.eclipse.paho.client.mqttv3.*
import java.time.LocalDateTime
import java.util.*

var devAmount: Long = 3
var devMinPeriod: Long = 5000
var devMaxPeriod: Long = 10000
var devIterations: Long = Long.MAX_VALUE / devMaxPeriod

fun main(args: Array<String>) {
    for (arg: String in args) {
        val optName: String = arg.split("=")[0].toLowerCase()
        val optValue: String = arg.split("=")[1].toLowerCase()

        when (optName) {
            "amount" -> {
                devAmount = optValue.toLong()
            }
            "min_period" -> {
                devMinPeriod = optValue.toLong()
            }
            "max_period" -> {
                devMaxPeriod = optValue.toLong()
            }
            "iterations" -> {
                devIterations = optValue.toLong()
            }
        }
    }

    if (devMaxPeriod < devMinPeriod) {
        var tmp: Long = devMaxPeriod
        devMaxPeriod = devMinPeriod
        devMinPeriod = devMaxPeriod
    }

    runDevices()
}

fun getGeneratedData(): String {
    val temperature: Long = (Math.random() * 300 + 300).toLong()
    val pressure: Long = (Math.random() * 40 + 740).toLong()
    val moisture: Long = (Math.random() * 40 + 30).toLong()
    val luminosity: Long = (Math.random() * 40 + 30).toLong()

    return "t=$temperature;p=$pressure;m=$moisture;l=$luminosity"
}

fun runDevices() {

    GlobalScope.launch {
        while (devAmount > 0) {
            devAmount -= 1
            GlobalScope.launch {
                val delay: Long = (Math.random() * (devMaxPeriod - devMinPeriod) + devMinPeriod).toLong()
                val devSignature = "dev_" + (devAmount + 1)
                var iterator: Int = devIterations.toInt()

                println("!D#$devSignature#$delay")

                while (iterator > 0) {
                    iterator -= 1
                    val generatedData: String = getGeneratedData()
                    val topicName: String = "/devices/evt/$devSignature"

                    val clientId: String = UUID.randomUUID().toString()
                    val client: IMqttClient = MqttClient("tcp://localhost:1883", clientId)

                    val mqttConnectOptions: MqttConnectOptions = MqttConnectOptions()
                    mqttConnectOptions.isAutomaticReconnect = true;
                    mqttConnectOptions.isCleanSession = true;
                    mqttConnectOptions.connectionTimeout = 10;

                    val topic: MqttTopic = client.getTopic(topicName)
                    val message: MqttMessage = MqttMessage(generatedData.toByteArray())
                    message.qos = 1
                    message.isRetained = true

                    client.connect(mqttConnectOptions);
                    topic.publish(message)

                    val now = LocalDateTime.now()
                    println("!A#$devSignature#$now#$iterator#$topicName#$generatedData")

                    client.disconnect()

                    delay(delay)
                }

                println("!R#$devSignature")
            }
            delay(10)
        }
    }

    Thread.sleep(devIterations * devMaxPeriod + devMaxPeriod * 3)
}
