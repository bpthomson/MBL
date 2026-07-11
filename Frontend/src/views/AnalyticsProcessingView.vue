<script setup>
import { computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import StreamingLog from '../components/StreamingLog.vue'

const route = useRoute()
const router = useRouter()

const userId = computed(() => route.query.user_id || '')
const sseUrl = computed(() => `/api/stream/analytics?user_id=${encodeURIComponent(userId.value)}`)

const handleDone = () => {
  router.push(`/analytics/${encodeURIComponent(userId.value)}`)
}
</script>

<template>
  <div class="flex-center">
    <StreamingLog 
      :sseUrl="sseUrl" 
      title="Generating Analytics..." 
      subtitle="Querying MAL API for detailed stats."
      @done="handleDone"
    />
  </div>
</template>

<style scoped>
</style>
