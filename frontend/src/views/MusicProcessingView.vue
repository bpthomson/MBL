<script setup>
import { computed, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import StreamingLog from '../components/StreamingLog.vue'

const route = useRoute()
const router = useRouter()
const userId = computed(() => route.query.user_id || '')
const sseUrl = computed(() => `/api/stream/music?user_id=${encodeURIComponent(userId.value)}`)

const downloadLink = ref(null)
const streamDone = ref(false)
const streamError = ref(false)

const handleDone = (messages) => {
  streamDone.value = true
  const finalMsg = messages.find(m => m.done && m.filename)
  if (finalMsg) {
    downloadLink.value = `/api/download/${finalMsg.filename}`
  }
}

const handleError = () => {
  streamError.value = true
}

const terminateAndReturn = () => {
  router.push(`/select/${encodeURIComponent(userId.value)}`)
}
</script>

<template>
  <StreamingLog 
    :sseUrl="sseUrl" 
    title="Compiling Audio Archive..." 
    subtitle="Please wait. Processing data stream."
    @done="handleDone"
    @error="handleError"
  >
    <template #default="{ isDone, error }">
      <div v-if="isDone && downloadLink" class="main-actions" style="margin-top: 20px;">
        <a :href="downloadLink" class="btn btn-primary" style="flex: 1; text-decoration: none;">Download Archive</a>
        <router-link :to="`/select/${userId}`" class="btn btn-secondary" style="flex: 1; text-decoration: none;">Return</router-link>
      </div>
      <div v-else class="main-actions" style="margin-top: 20px;">
        <button @click="terminateAndReturn" class="btn btn-outline" style="flex: 1;">
          {{ error ? 'Return to Selection' : '⏹ Terminate & Return' }}
        </button>
      </div>
    </template>
  </StreamingLog>
</template>

<style scoped>
/* Scoped styles empty to inherit from global style.css */
</style>
