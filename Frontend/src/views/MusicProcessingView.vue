<script setup>
import { computed, ref } from 'vue'
import { useRoute } from 'vue-router'
import StreamingLog from '../components/StreamingLog.vue'

const route = useRoute()
const userId = computed(() => route.query.user_id || '')
const sseUrl = computed(() => `/api/stream/music?user_id=${encodeURIComponent(userId.value)}`)

const downloadLink = ref(null)

const handleDone = (messages) => {
  const finalMsg = messages.find(m => m.done && m.filename)
  if (finalMsg) {
    downloadLink.value = `/api/download/${finalMsg.filename}`
  }
}
</script>

<template>
  <StreamingLog 
    :sseUrl="sseUrl" 
    title="Compiling Audio Archive..." 
    subtitle="Please wait. Processing data stream."
    @done="handleDone"
  >
    <template #default="{ isDone, error }">
      <div v-if="isDone && downloadLink" class="main-actions" style="margin-top: 20px;">
        <a :href="downloadLink" class="btn btn-primary" style="flex: 1; text-decoration: none;">Download Archive</a>
        <router-link :to="`/select/${userId}`" class="btn btn-secondary" style="flex: 1; text-decoration: none;">Return</router-link>
      </div>
      <div v-else-if="error" class="main-actions" style="margin-top: 20px;">
        <router-link :to="`/select/${userId}`" class="btn btn-secondary" style="flex: 1; text-decoration: none;">Return</router-link>
      </div>
    </template>
  </StreamingLog>
</template>

<style scoped>
/* Scoped styles empty to inherit from global style.css */
</style>
