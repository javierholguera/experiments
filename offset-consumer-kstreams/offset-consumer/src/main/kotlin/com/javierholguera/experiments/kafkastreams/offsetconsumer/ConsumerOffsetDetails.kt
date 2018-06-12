package com.javierholguera.experiments.kafkastreams.offsetconsumer

data class ConsumerOffsetDetails(
        val topic: String,
        val partition: Int,
        val group: String,
        val version: Int,
        val offset: Long?,
        val metadata: String?,
        val commitTimestamp: Long?,
        val expireTimestamp: Long?
)