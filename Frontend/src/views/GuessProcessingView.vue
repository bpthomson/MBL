<script setup>
import { computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import StreamingLog from '../components/StreamingLog.vue'

const route = useRoute()
const router = useRouter()

const userId = computed(() => route.query.user_id || '')
const sseUrl = computed(() => `/api/stream/guess-playlist?user_id=${encodeURIComponent(userId.value)}`)

const handleDone = () => {
  router.push({ path: '/guess-game', query: { user_id: userId.value } })
}
</script>

<template>
  <div class="flex-center">
    <StreamingLog 
      :sseUrl="sseUrl" 
      title="Initializing Quiz Module..." 
      subtitle="Fetching metadata and preview streams."
      @done="handleDone"
    />
  </div>
</template>

<style scoped>
</style>
