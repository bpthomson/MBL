<script setup>
import { onMounted, watch, ref } from 'vue'
import { useSSE } from '../composables/useSSE'

const props = defineProps({ sseUrl: String, title: String, subtitle: String })
const emit = defineEmits(['done', 'error'])

const { connect, messages, error, isDone, progress } = useSSE()
const logEl = ref(null)
const statusMessage = ref(props.title || 'Compiling Data...')

onMounted(() => { connect(props.sseUrl) })

watch(isDone, (val) => { if (val && !error.value) emit('done', messages.value) })
watch(error, (val) => { if (val) emit('error', val) })

watch(messages, (newMsgs) => {
  const latest = newMsgs[newMsgs.length - 1]
  if (latest) {
    if (latest.error) {
      statusMessage.value = "System Error"
    } else if (latest.done) {
      statusMessage.value = "Archive Ready."
    } else if (latest.msg) {
      statusMessage.value = latest.msg
    }
  }
  
  setTimeout(() => {
    if (logEl.value) {
      logEl.value.scrollTop = logEl.value.scrollHeight
    }
  }, 50)
}, { deep: true })
</script>

<template>
  <div class="wrapper">
      <div class="header-section col">
          <h1 :class="{'text-success': isDone && !error, 'text-error': error}">{{ error ? 'System Error' : statusMessage }}</h1>
          <p class="sub-text">{{ subtitle || 'Please wait. Processing data stream.' }}</p>
      </div>
      <div class="progress-bg"><div class="progress-fill" :style="{ width: progress || '0%' }"></div></div>
      <div class="log-window" ref="logEl">
        <div v-if="error" class="log-line">> SYS_ERR: {{ error }}</div>
        <div v-for="(msg, i) in messages" :key="i">
          <div v-if="msg.msg" class="log-line">> {{ msg.msg }}</div>
          <div v-if="msg.error" class="log-line">> SYS_ERR: {{ msg.error }}</div>
        </div>
      </div>
      <slot :isDone="isDone && !error" :error="!!error"></slot>
  </div>
</template>

<style scoped>
/* Inherits styling from global style.css wrapper, header-section, progress-bg, log-window, etc */
</style>
