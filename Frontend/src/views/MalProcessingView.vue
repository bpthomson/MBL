<script setup>
import { computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import StreamingGallery from '../components/StreamingGallery.vue'

const route = useRoute()
const router = useRouter()

const userId = computed(() => route.query.user_id || '')

const sseUrl = computed(() => {
  return `/api/stream/mal-import?user_id=${encodeURIComponent(userId.value)}`
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
