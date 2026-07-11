<script setup>
import { computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import StreamingGallery from '../components/StreamingGallery.vue'

const route = useRoute()
const router = useRouter()

const userId = computed(() => route.query.user_id || '')
const limit = computed(() => route.query.limit || '')

const sseUrl = computed(() => {
  return `/api/stream/progress?user_id=${encodeURIComponent(userId.value)}&limit=${encodeURIComponent(limit.value)}`
})

const handleDone = () => {
  router.push(`/select/${encodeURIComponent(userId.value)}`)
}
</script>

<template>
  <div class="flex-center">
    <StreamingGallery 
      :sseUrl="sseUrl" 
      @done="handleDone" 
    />
  </div>
</template>

<style scoped>
</style>
