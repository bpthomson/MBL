<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useApi } from '../composables/useApi'

const route = useRoute()
const router = useRouter()
const { get, post } = useApi()

const userId = computed(() => route.query.user_id || '')
const defMin = Number(route.query.def_min || 2000)
const defMax = Number(route.query.def_max || new Date().getFullYear())

const sliderMinVal = ref(defMin)
const sliderMaxVal = ref(defMax)
const includeNa = ref(true)
const errorMessage = ref('')

const finalMin = computed(() => Math.min(sliderMinVal.value, sliderMaxVal.value))
const finalMax = computed(() => Math.max(sliderMinVal.value, sliderMaxVal.value))

const yearsData = ref([])
const naCount = ref(0)
const dynamicTotal = computed(() => {
  if (!yearsData.value.length && !naCount.value) return Number(route.query.total || 0)
  let count = yearsData.value.filter(y => y >= finalMin.value && y <= finalMax.value).length
  if (includeNa.value) count += naCount.value
  return count
})

onMounted(async () => {
  try {
    const res = await get('/api/guess/preview')
    if (res.ok) {
      yearsData.value = res.years
      naCount.value = res.na_count
    }
  } catch(e) { console.error(e) }
})

const sliderTrackStyle = computed(() => {
  if (defMax > defMin) {
    const pct1 = ((finalMin.value - defMin) / (defMax - defMin)) * 100
    const pct2 = ((finalMax.value - defMin) / (defMax - defMin)) * 100
    return {
      left: pct1 + '%',
      width: (pct2 - pct1) + '%'
    }
  }
  return { left: '0%', width: '100%' }
})

const submitSetup = async () => {
  try {
    const res = await post('/api/guess/start', {
      user_id: userId.value,
      min_year: finalMin.value,
      max_year: finalMax.value,
      include_na: includeNa.value
    })
    
    if (res.ok) {
      router.push({ path: '/guess-processing', query: { user_id: userId.value } })
    } else {
      errorMessage.value = res.error || 'Setup failed'
    }
  } catch (e) {
    errorMessage.value = "Error: " + e.message
  }
}
</script>

<template>
  <div class="flex-center">
    <div class="wrapper" style="display: flex; flex-direction: column; align-items: center;">
      <div class="header-section" style="text-align: center;">
        <h1>Module Configuration</h1>
        <p class="sub-text">Selected candidates: <span style="color: #58a6ff; font-weight: bold;">{{ dynamicTotal }}</span></p>
        <p v-if="errorMessage" style="color: #ff7b72; font-size: 13px; margin-top: 10px;">{{ errorMessage }}</p>
      </div>

      <div class="setup-card">
        <form @submit.prevent="submitSetup">
          <div class="form-group">
            <label>> YEAR RANGE</label>
            <div class="range-slider-container">
              <div style="position: absolute; width: 100%; height: 4px; background: #30363d; top: 10px; border-radius: 2px;"></div>
              <div :style="sliderTrackStyle" style="position: absolute; height: 4px; background: #005bb5; top: 10px; border-radius: 2px; pointer-events: none;"></div>
              <input type="range" v-model.number="sliderMinVal" :min="defMin" :max="defMax">
              <input type="range" v-model.number="sliderMaxVal" :min="defMin" :max="defMax">
            </div>
            <div style="display: flex; justify-content: space-between; color: #c9d1d9; font-size: 13px; font-family: monospace;">
              <span>{{ finalMin }}</span>
              <span>{{ finalMax }}</span>
            </div>
          </div>

          <div class="form-group">
            <label class="checkbox-group">
              <input type="checkbox" v-model="includeNa">
              Include entries missing year data (N/A)
            </label>
          </div>

          <div style="display: flex; gap: 10px; margin-top: 30px;">
            <router-link :to="`/select/${userId}`" class="btn btn-outline" style="flex: 1; text-align: center; text-decoration: none;">Back</router-link>
            <button type="submit" class="btn btn-primary" style="flex: 2;">Initialize DB</button>
          </div>
        </form>
      </div>
    </div>
  </div>
</template>

<style>
.setup-card { background: #0d1117; padding: 25px; border-radius: 8px; border: 1px solid #30363d; width: 100%; max-width: 400px; text-align: left; }
.form-group { margin-bottom: 20px; }
.form-group label { display: block; color: #8b949e; font-size: 12px; font-family: monospace; margin-bottom: 8px; }
.checkbox-group { display: flex; align-items: center; gap: 8px; color: #c9d1d9; font-size: 13px; cursor: pointer; }

/* 雙向滑桿樣式 */
.range-slider-container { position: relative; width: 100%; height: 40px; margin-top: 15px; }
.range-slider-container input[type=range] { position: absolute; width: 100%; top: 2px; -webkit-appearance: none; appearance: none; background: transparent; pointer-events: none; margin: 0; padding: 0; outline: none; }
.range-slider-container input[type=range]::-webkit-slider-thumb { pointer-events: auto; -webkit-appearance: none; appearance: none; height: 16px; width: 16px; border-radius: 50%; background: #c9d1d9; cursor: pointer; position: relative; z-index: 10; box-shadow: 0 0 4px rgba(0,0,0,0.5); }
.range-slider-container input[type=range]::-moz-range-thumb { pointer-events: auto; height: 16px; width: 16px; border-radius: 50%; background: #c9d1d9; cursor: pointer; position: relative; z-index: 10; box-shadow: 0 0 4px rgba(0,0,0,0.5); border: none; }
.range-slider-container input[type=range]:active::-webkit-slider-thumb { z-index: 20; background: #ffffff; }
.range-slider-container input[type=range]:active::-moz-range-thumb { z-index: 20; background: #ffffff; }
</style>
