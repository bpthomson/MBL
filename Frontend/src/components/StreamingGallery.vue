<script setup>
import { ref, onMounted, watch } from 'vue'
import { useSSE } from '../composables/useSSE'

const props = defineProps({ sseUrl: String })
const emit = defineEmits(['done', 'error'])

const { connect, messages, error, isDone } = useSSE()
const images = ref([])
const totalItems = ref(0)
const currentItem = ref(0)
const galleryEl = ref(null)
const statusMessage = ref('Establishing Connection...')
const statusColor = ref('#c9d1d9')

onMounted(() => { connect(props.sseUrl) })

watch(isDone, (val) => { 
  if (val && !error.value) {
    statusMessage.value = "Task Completed. Redirecting..."
    statusColor.value = "#3fb950"
    emit('done', messages.value) 
  }
})

watch(error, (val) => { 
  if (val) {
    statusMessage.value = `ERR: ${val}`
    statusColor.value = "#ff7b72"
    emit('error', val) 
  }
})

watch(messages, (newMsgs) => {
  const latest = newMsgs[newMsgs.length - 1]
  if (latest) {
    if (latest.error) {
      statusMessage.value = `ERR: ${latest.error}`
      statusColor.value = "#ff7b72"
    } else {
      if (latest.msg) statusMessage.value = latest.msg
      if (latest.type === 'image') {
        images.value.push(latest)
        if (latest.total) totalItems.value = latest.total
        if (latest.current) currentItem.value = latest.current
        
        setTimeout(() => {
          if (galleryEl.value) {
            const isAtRightEnd = (galleryEl.value.scrollWidth - galleryEl.value.scrollLeft - galleryEl.value.clientWidth) < 50
            if (isAtRightEnd) {
              galleryEl.value.scrollTo({ left: galleryEl.value.scrollWidth, behavior: 'smooth' })
            }
          }
        }, 50)
      }
    }
  }
}, { deep: true })
</script>

<template>
  <div style="text-align: center; margin: 30px 0;">
      <div v-if="!isDone && !error" class="loader" style="margin: 0 auto 15px auto;"></div>
      <h2 :style="{ fontSize: '16px', fontWeight: '500', color: statusColor, margin: '0 0 5px 0' }">{{ statusMessage }}</h2>
      <div v-if="!error" style="font-size: 13px; color: #8b949e; font-family: monospace;">
        {{ totalItems > 0 ? `[ ${currentItem} / ${totalItems} ]` : 'Awaiting data stream' }}
      </div>
  </div>
  <div class="gallery-container" ref="galleryEl">
      <div v-for="d in images" :key="d.current" class="img-card" :class="{ 'low': d.is_low }">
          <div class="status-tag" v-show="d.is_low">{{ d.status.includes('Low') ? 'WARN: Low Accuracy' : d.status }}</div>
          <img :src="d.img_url">
          <div class="card-title">{{ d.title }}</div>
      </div>
  </div>
</template>

<style scoped>
.gallery-container { width: 90%; height: 320px; display: flex; align-items: center; overflow-x: auto; background: #161b22; border-radius: 8px; padding: 0 20px; border: 1px solid #30363d; scroll-behavior: smooth; margin: 0 auto; }
.gallery-container::-webkit-scrollbar { height: 6px; }
.gallery-container::-webkit-scrollbar-thumb { background: #30363d; border-radius: 3px; }

.img-card { flex: 0 0 auto; width: 200px; margin-right: 20px; position: relative; background: #0d1117; border-radius: 6px; overflow: hidden; border: 1px solid #30363d; animation: slideInRight 0.4s ease-out forwards; }
.img-card img { width: 100%; height: 280px; object-fit: cover; display: block; opacity: 0; animation: fadeIn 0.4s ease-out 0.1s forwards; filter: brightness(0.85); }
.card-title { position: absolute; bottom: 0; width: 100%; background: rgba(13,17,23,0.9); color: #c9d1d9; font-size: 12px; padding: 10px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; box-sizing: border-box; border-top: 1px solid #30363d;}
.status-tag { position: absolute; top: 0; width: 100%; background: rgba(218, 54, 51, 0.9); color: white; font-size: 10px; padding: 3px 0; text-align: center; display: none; font-family: monospace; }
.img-card.low { border-color: #da3633; }
.img-card.low .status-tag { display: block; }
@keyframes slideInRight { from { transform: translateX(30px); opacity: 0; } to { transform: translateX(0); opacity: 1; } }
@keyframes fadeIn { to { opacity: 1; } }
</style>
