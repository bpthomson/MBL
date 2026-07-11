<script setup>
import { ref, onMounted, computed, nextTick } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useApi } from '../composables/useApi'

const route = useRoute()
const router = useRouter()
const { get } = useApi()

const userId = computed(() => route.query.user_id || '')
const gamePlaylist = ref([])
const statusMessage = ref('Initializing...')
const statusClasses = ref('sub-text')

const isAnswerRevealed = ref(false)
const isPreloading = ref(false)
const isNextSongReady = ref(false)

const currentSong = ref(null)
const nextSong = ref(null)

const guessName = ref('')
const guessType = ref('')
const guessYear = ref('')
const nameClass = ref('')
const typeClass = ref('')
const yearClass = ref('')

const isAudioReady = ref(false)
const uniqueTitles = ref([])

const audioPlayerEl = ref(null)
const videoPlayerEl = ref(null)
const plotWidth = ref('0%')
const charWidth = ref('0%')
const atmoWidth = ref('0%')

let playbackTimeout = null

const setStatus = (msg, isError = false, isCorrect = false) => {
  statusMessage.value = msg
  statusClasses.value = 'sub-text'
  if (isError) statusClasses.value += ' text-error'
  if (isCorrect) statusClasses.value += ' text-success'
}

onMounted(async () => {
  try {
    const res = await get('/api/guess/playlist')
    if (res.ok && res.playlist && res.playlist.length > 0) {
      gamePlaylist.value = res.playlist
      uniqueTitles.value = [...new Set(gamePlaylist.value.map(item => item.anime_ch_name))]
      setupNewGame()
    } else {
      setStatus("SYS_ERR: Missing dataset.", true)
    }
  } catch (e) {
    setStatus("SYS_ERR: Connection failed.", true)
  }
})

const preloadNextSong = async () => {
  if (isPreloading.value || gamePlaylist.value.length === 0) return
  isPreloading.value = true
  try {
    const metadata = gamePlaylist.value[Math.floor(Math.random() * gamePlaylist.value.length)]
    const proxyUrl = `/api/audio-proxy?url=${encodeURIComponent(metadata.theme_link)}`
    const audioResponse = await fetch(proxyUrl)
    if (!audioResponse.ok) throw new Error('Fetch failed')
    
    const arrayBuffer = await audioResponse.arrayBuffer()
    let startOffset = -1
    
    const audioCtx = new (window.AudioContext || window.webkitAudioContext)()
    let audioBuffer
    try {
      const chunk = arrayBuffer.slice(0, 96 * 1024)
      audioBuffer = await audioCtx.decodeAudioData(chunk)
    } catch (err) {
      audioBuffer = await audioCtx.decodeAudioData(arrayBuffer.slice(0))
    }
    const channelData = audioBuffer.getChannelData(0)
    const threshold = 0.002
    
    for (let i = 0; i < channelData.length; i++) {
      if (Math.abs(channelData[i]) > threshold) {
        startOffset = i / audioBuffer.sampleRate
        break
      }
    }

    if (startOffset < 0) {
      startOffset = channelData.length / audioBuffer.sampleRate
    }

    if(audioCtx.state !== 'closed') audioCtx.close()

    const audioBlob = new Blob([arrayBuffer])
    nextSong.value = { metadata, audioBlobUrl: URL.createObjectURL(audioBlob), startOffset: startOffset }
  } catch (error) { 
    nextSong.value = null 
  } finally { 
    isPreloading.value = false 
  }
}

const setupNewGame = async () => {
  isAnswerRevealed.value = false
  isAudioReady.value = false
  isNextSongReady.value = false
  setStatus('Loading...')
  
  guessName.value = ''
  guessType.value = ''
  guessYear.value = ''
  nameClass.value = ''
  typeClass.value = ''
  yearClass.value = ''
  
  if (playbackTimeout) clearTimeout(playbackTimeout)
  if (currentSong.value && currentSong.value.audioBlobUrl) URL.revokeObjectURL(currentSong.value.audioBlobUrl)
  if (audioPlayerEl.value) audioPlayerEl.value.pause()
  
  plotWidth.value = '0%'
  charWidth.value = '0%'
  atmoWidth.value = '0%'

  if (!nextSong.value) await preloadNextSong()
  currentSong.value = nextSong.value
  nextSong.value = null
  
  if (!currentSong.value) { 
    setStatus('SYS_ERR: Load failed.', true)
    isAudioReady.value = true
    isNextSongReady.value = true
    return 
  }
  
  const preloadPromise = preloadNextSong() 
  if (audioPlayerEl.value) {
    audioPlayerEl.value.src = currentSong.value.audioBlobUrl
    audioPlayerEl.value.load()
    audioPlayerEl.value.oncanplaythrough = async () => {
      setStatus('Ready.')
      isAudioReady.value = true
      await preloadPromise
      isNextSongReady.value = true
    }
    audioPlayerEl.value.onerror = () => { 
      setStatus('SYS_ERR: Playback exception.', true)
      isAudioReady.value = true 
      isNextSongReady.value = true
    }
  }
}

const checkAnswer = () => {
  if (isAnswerRevealed.value || !currentSong.value) return
  const { metadata } = currentSong.value
  let correctCount = 0
  
  const nameCorrect = guessName.value.trim() === metadata.anime_ch_name
  nameClass.value = nameCorrect ? 'correct' : 'incorrect'
  if (nameCorrect) correctCount++
  
  const typeCorrect = guessType.value.trim().toUpperCase() === metadata.theme_type.toUpperCase()
  typeClass.value = typeCorrect ? 'correct' : 'incorrect'
  if (typeCorrect) correctCount++
  
  const yearCorrect = guessYear.value.trim() === String(metadata.anime_year)
  yearClass.value = yearCorrect ? 'correct' : 'incorrect'
  if (yearCorrect) correctCount++
  
  if (correctCount === 3) { 
    setStatus('Match: 100%.', false, true)
    revealAnswer() 
  } else { 
    setStatus(`Match: ${correctCount} / 3.`, correctCount === 0) 
  }
}

const revealAnswer = () => {
  if (isAnswerRevealed.value || !currentSong.value) return
  isAnswerRevealed.value = true
  if (playbackTimeout) clearTimeout(playbackTimeout)
  if (audioPlayerEl.value) audioPlayerEl.value.pause()
  
  const { metadata } = currentSong.value
  
  if (metadata.review_content) {
    let cleanReview = metadata.review_content
    const scoreRegex = />>>[\s\S]*?劇情\s*([0-9.]+)\s*人設\s*([0-9.]+)\s*氛圍\s*([0-9.]+)\s*總評\s*([0-9.]+)/
    const match = cleanReview.match(scoreRegex)

    if (match) {
      const plotPct = Math.min((parseFloat(match[1]) / 3) * 100, 100)
      const charPct = Math.min((parseFloat(match[2]) / 3) * 100, 100)
      const atmoPct = Math.min((parseFloat(match[3]) / 3) * 100, 100)
      
      setTimeout(() => {
        plotWidth.value = plotPct + '%'
        charWidth.value = charPct + '%'
        atmoWidth.value = atmoPct + '%'
      }, 50)
    }
  }
  
  nextTick(() => {
    if (metadata.video_link && videoPlayerEl.value) {
      videoPlayerEl.value.play()
    }
  })
}

const playAudio = (seconds, isRandom) => {
  if (!currentSong.value || !audioPlayerEl.value) return
  if (playbackTimeout) clearTimeout(playbackTimeout)
  let startTime = currentSong.value.startOffset || 0
  if (isRandom && audioPlayerEl.value.duration) { 
    const maxStartTime = audioPlayerEl.value.duration - seconds
    startTime = Math.random() * (maxStartTime > 0 ? maxStartTime : 0) 
  }
  audioPlayerEl.value.currentTime = startTime 
  audioPlayerEl.value.play()
  playbackTimeout = setTimeout(() => {
    if(audioPlayerEl.value) audioPlayerEl.value.pause()
  }, seconds * 1000)
}

// Computed properties for the template
const reviewData = computed(() => {
  if (!currentSong.value || !currentSong.value.metadata) return null
  const { metadata } = currentSong.value
  if (!metadata.review_content) return null
  
  let cleanReview = metadata.review_content
  const scoreRegex = />>>[\s\S]*?劇情\s*([0-9.]+)\s*人設\s*([0-9.]+)\s*氛圍\s*([0-9.]+)\s*總評\s*([0-9.]+)/
  const match = cleanReview.match(scoreRegex)
  
  let scores = null
  if (match) {
    cleanReview = cleanReview.replace(match[0], '').trim()
    scores = { plot: match[1], char: match[2], atmo: match[3], total: match[4] }
  }
  
  return { cleanReview, scores }
})

const noReviewQueueMsg = computed(() => {
  const n = Math.floor(Math.random() * 900000) + 100000
  return `BpThomson 已將這部作品排入待看清單中的第 ${n.toLocaleString()} 位。`
})

const animeTitleDisplay = computed(() => {
  if (!currentSong.value) return ''
  const { metadata } = currentSong.value
  let titleText = metadata.anime_ch_name
  if (metadata.anime_year && metadata.anime_year !== 'N/A') { 
    titleText += ` (${metadata.anime_year})` 
  }
  return titleText
})
</script>

<template>
  <div class="flex-center">
    <div class="main-layout">
      
      <div v-show="isAnswerRevealed" id="review-column" class="side-column" :style="{ borderLeft: reviewData ? '3px solid #3fb950' : '3px solid #58a6ff' }" style="display: flex; flex-direction: column;">
        
        <div v-if="reviewData" style="display: flex; flex-direction: column;">
          <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 15px;">
              <img src="https://avatar2.bahamut.com.tw/avataruserpic/s/s/sses3205/sses3205.png" alt="Avatar" style="width: 36px; height: 36px; border-radius: 50%; border: 1px solid #3fb950; box-shadow: 0 0 8px rgba(63, 185, 80, 0.3);">
              <p style="color: #8b949e; font-size: 12px; margin: 0; font-family: monospace; font-weight: bold;">> Review (BpThomson):</p>
          </div>
          <p style="color: #c9d1d9; font-size: 16px; margin: 0; line-height: 1.6; white-space: pre-wrap;">{{ reviewData.cleanReview }}</p>
          
          <div v-if="reviewData.scores" style="margin-top: 25px; padding-top: 20px; border-top: 1px dashed #30363d;">
              <div class="score-bar-group">
                  <span class="score-label">劇情</span>
                  <div class="score-bar-bg"><div class="score-bar-fill" :style="{ width: plotWidth }"></div></div>
                  <span class="score-val">{{ reviewData.scores.plot }}</span>
              </div>
              <div class="score-bar-group">
                  <span class="score-label">人設</span>
                  <div class="score-bar-bg"><div class="score-bar-fill" :style="{ width: charWidth }"></div></div>
                  <span class="score-val">{{ reviewData.scores.char }}</span>
              </div>
              <div class="score-bar-group">
                  <span class="score-label">氛圍</span>
                  <div class="score-bar-bg"><div class="score-bar-fill" :style="{ width: atmoWidth }"></div></div>
                  <span class="score-val">{{ reviewData.scores.atmo }}</span>
              </div>
              <div style="text-align: center; margin-top: 25px;">
                  <p style="color: #8b949e; font-size: 12px; margin: 0 0 5px 0; font-family: monospace;">> OVERALL RATING</p>
                  <div class="artistic-total">{{ reviewData.scores.total }}</div>
              </div>
          </div>
        </div>

        <div v-else style="flex: 1; display: flex; flex-direction: column; align-items: center; justify-content: center; opacity: 0.9; min-height: 250px; padding: 20px; text-align: center;">
          <div style="font-size: 40px; margin-bottom: 20px; color: #58a6ff; font-family: monospace; font-weight: bold; letter-spacing: 2px;">[QUEUE]</div>
          <p style="color: #e6edf3; font-size: 16px; line-height: 1.8; font-weight: bold; margin: 0;">{{ noReviewQueueMsg }}</p>
          <p style="color: #8b949e; font-size: 12px; margin-top: 25px; font-family: monospace;">[ 查無 BpThomson 的觀看紀錄 ]</p>
        </div>
      </div>

      <div class="wrapper" style="margin: 0 !important; flex: 0 0 520px;">
        <div class="header-section">
            <h1>Audio Quiz Module</h1>
            <p :class="statusClasses">{{ statusMessage }}</p>
        </div>

        <audio ref="audioPlayerEl" controls></audio>

        <div class="controls-group">
            <p>> Standard Playback</p>
            <div class="controls-container">
                <button class="play-btn play-btn-1" :disabled="!isAudioReady || isAnswerRevealed" @click="playAudio(1, false)">1.0s</button>
                <button class="play-btn play-btn-2" :disabled="!isAudioReady || isAnswerRevealed" @click="playAudio(3, false)">3.0s</button>
                <button class="play-btn play-btn-3" :disabled="!isAudioReady || isAnswerRevealed" @click="playAudio(10, false)">10.0s</button>
            </div>
        </div>

        <div class="controls-group">
            <p>> Random Playback</p>
            <div class="controls-container">
                <button class="play-btn play-btn-1" :disabled="!isAudioReady || isAnswerRevealed" @click="playAudio(1, true)">1.0s</button>
                <button class="play-btn play-btn-2" :disabled="!isAudioReady || isAnswerRevealed" @click="playAudio(3, true)">3.0s</button>
                <button class="play-btn play-btn-3" :disabled="!isAudioReady || isAnswerRevealed" @click="playAudio(10, true)">10.0s</button>
            </div>
        </div>

        <div id="answer-inputs-container">
            <input type="text" v-model="guessName" :class="nameClass" :disabled="isAnswerRevealed" @keypress.enter="checkAnswer" placeholder="Title" list="anime-titles-list" autocomplete="off">
            <datalist id="anime-titles-list">
              <option v-for="title in uniqueTitles" :key="title" :value="title"></option>
            </datalist>
            <input type="text" v-model="guessType" :class="typeClass" :disabled="isAnswerRevealed" @keypress.enter="checkAnswer" placeholder="Type (e.g. OP1)" autocomplete="off">
            <input type="text" v-model="guessYear" :class="yearClass" :disabled="isAnswerRevealed" @keypress.enter="checkAnswer" placeholder="Year" autocomplete="off">
        </div>

        <div class="main-actions">
            <button class="btn btn-primary" :disabled="!isAudioReady || isAnswerRevealed" @click="checkAnswer">Verify</button>
            <button class="btn btn-secondary" :disabled="!isAudioReady || isAnswerRevealed" @click="revealAnswer">Reveal</button>
            <button class="btn btn-success" :disabled="!isNextSongReady" @click="setupNewGame">Next Track</button>
        </div>
        
        <div style="margin-top: 30px; text-align: center;">
            <router-link :to="`/select/${userId}`" style="color: #6e7681; text-decoration: none; font-size: 12px; font-family: monospace;">[ Return to Selection ]</router-link>
        </div>
      </div>

      <div v-show="isAnswerRevealed" id="answer-column" class="side-column" style="display: block; border-top: 3px solid #58a6ff;">
          <h2 style="color: #fff; margin: 0 0 5px 0; font-size: 16px; font-weight: 500;">{{ animeTitleDisplay }}</h2>
          <p style="color: #8b949e; margin: 0 0 15px 0; font-family: monospace; font-size: 12px;">> {{ currentSong?.metadata.theme_type }} : {{ currentSong?.metadata.theme_title }}</p>
          <video v-if="currentSong?.metadata.video_link" ref="videoPlayerEl" :src="currentSong?.metadata.video_link" controls style="display: block;"></video>
          <img v-else-if="currentSong?.metadata.anime_img_url" :src="currentSong?.metadata.anime_img_url" alt="Cover" style="display: block;">
      </div>

    </div>
  </div>
</template>

<style scoped>
audio { width: 100%; height: 36px; margin-bottom: 25px; outline: none; border-radius: 4px; opacity: 0.8; transition: 0.2s; }
audio:hover { opacity: 1; }

.controls-group { margin-bottom: 18px; background: #0d1117; padding: 15px; border-radius: 6px; border: 1px solid #21262d; }
.controls-group p { margin: 0 0 10px 0; font-size: 11px; color: #8b949e; font-family: monospace; letter-spacing: 0.5px; }
.controls-container { display: flex; justify-content: center; gap: 8px; flex-wrap: wrap; }

#answer-inputs-container { display: flex; flex-direction: column; gap: 12px; margin: 25px 0; }
#answer-inputs-container input { margin: 0; }

.main-actions { display: flex; gap: 10px; justify-content: center; }
.main-actions button { flex: 1; margin: 0; }

.main-layout {
    display: flex;
    gap: 20px;
    width: 98%; 
    max-width: 1800px; 
    margin: 20px auto;
    padding: 0 10px;
    align-items: flex-start;
    justify-content: center;
}

.side-column {
    flex: 1 1 300px; 
    min-width: 280px; 
    background: #0d1117;
    border: 1px solid #30363d;
    border-radius: 8px;
    padding: 20px;
    box-shadow: 0 4px 12px rgba(0,0,0,0.2);
    animation: fadeIn 0.4s ease-out;
    box-sizing: border-box; 
}

.wrapper {
    flex: 0 0 520px; 
    margin: 0 !important;
    box-sizing: border-box;
}

img { width: 100%; max-height: 400px; object-fit: cover; border-radius: 6px; border: 1px solid #30363d; }
video { width: 100%; border-radius: 6px; outline: none; background: #000; }

@keyframes fadeIn { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }

/* 評分長條圖與藝術字體樣式 */
.score-bar-group { display: flex; align-items: center; margin-bottom: 12px; gap: 10px; }
.score-label { color: #c9d1d9; font-size: 14px; font-weight: bold; width: 35px; }
.score-bar-bg { flex: 1; height: 14px; background: #21262d; border-radius: 7px; overflow: hidden; box-shadow: inset 0 1px 3px rgba(0,0,0,0.5); }
.score-bar-fill { height: 100%; background: linear-gradient(90deg, #3fb950, #2ea043); transition: width 0.6s cubic-bezier(0.22, 1, 0.36, 1); }
.score-val { color: #8b949e; font-size: 14px; font-family: monospace; width: 25px; text-align: right; }

.artistic-total {
    font-size: 56px;
    font-weight: 900;
    color: #ffd700;
    text-shadow: 0 0 15px rgba(255, 215, 0, 0.4), 2px 2px 0px #b8860b;
    font-family: 'Arial Black', Impact, sans-serif;
    line-height: 1;
    margin-top: 5px;
}

@media (max-width: 1200px) {
    .main-layout {
        flex-direction: column; 
        align-items: center;
        max-width: 700px; 
    }
    .wrapper { order: 1; width: 100%; flex: auto; }
    #answer-column { order: 2; width: 100%; }
    #review-column { order: 3; width: 100%; }
}
</style>
