<script setup>
import { ref, onMounted, computed, nextTick } from 'vue'
import { useRoute } from 'vue-router'
import { useApi } from '../composables/useApi'
import Chart from 'chart.js/auto'

const route = useRoute()
const { get } = useApi()
const userId = computed(() => route.params.userId || '')

const stats = ref(null)
const isLoading = ref(true)

const genreChartRef = ref(null)
const studioChartRef = ref(null)
const yearChartRef = ref(null)
const sourceChartRef = ref(null)
const demoChartRef = ref(null)
const epChartRef = ref(null)

const showModal = ref(false)
const modalTitle = ref('')
const modalItems = ref([])
const modalContentRef = ref(null)

onMounted(async () => {
  try {
    const data = await get('/api/analytics')
    if (data.ok && data.stats) {
      stats.value = data.stats
      isLoading.value = false
      nextTick(initCharts)
    } else {
      alert("No data available")
    }
  } catch (e) {
    alert("Error: " + e.message)
  }
})

const initCharts = () => {
  if (!stats.value) return
  
  Chart.defaults.color = '#8b949e';
  Chart.defaults.font.family = 'monospace';
  Chart.defaults.borderColor = '#21262d';
  
  const darkColors = [
    'rgba(88, 166, 255, 0.7)', 'rgba(63, 185, 80, 0.7)', 'rgba(163, 113, 247, 0.7)', 
    'rgba(210, 168, 255, 0.7)', 'rgba(121, 192, 255, 0.7)', 'rgba(255, 123, 114, 0.7)', 
    'rgba(255, 166, 87, 0.7)', 'rgba(137, 87, 229, 0.7)', 'rgba(46, 160, 67, 0.7)'
  ];

  const getSortedData = (obj) => {
    const entries = Object.entries(obj).sort((a, b) => b[1] - a[1]);
    return { labels: entries.map(e => e[0]), data: entries.map(e => e[1]) };
  }

  const handleChartClick = (filterType) => {
    return (e, elements, chart) => {
      if (!elements.length) return;
      const idx = elements[0].index;
      const label = chart.data.labels[idx];
      
      const rawData = stats.value.raw_data || [];
      let filtered = [];
      if (filterType === 'genre') filtered = rawData.filter(item => (item.genres || []).includes(label));
      else if (filterType === 'studio') filtered = rawData.filter(item => (item.studios || []).includes(label));
      else if (filterType === 'year') filtered = rawData.filter(item => String(item.year) === String(label));
      else if (filterType === 'source') filtered = rawData.filter(item => item.source === label);
      else if (filterType === 'demo') filtered = rawData.filter(item => (item.demographics || []).includes(label));
      else if (filterType === 'ep') {
        filtered = rawData.filter(item => {
          const eps = item.episodes || 0;
          if (label === "Movie/OVA (1)") return eps === 1;
          if (label === "Short (2-13)") return eps > 1 && eps <= 13;
          if (label === "Medium (14-26)") return eps > 13 && eps <= 26;
          if (label === "Long (27+)") return eps > 26;
          return false;
        });
      }

      filtered.sort((a, b) => (a.rank || 99999) - (b.rank || 99999));
      
      modalTitle.value = `> ${filterType.toUpperCase()}: ${label} (${filtered.length} TITLES)`;
      modalItems.value = filtered;
      showModal.value = true;
      
      setTimeout(() => { if (modalContentRef.value) modalContentRef.value.scrollTop = 0 }, 1);
    }
  }

  const getPieOptions = (filterType) => ({
    plugins: { legend: { position: 'right' } },
    elements: { arc: { borderWidth: 2, borderColor: '#0d1117' } },
    onClick: handleChartClick(filterType)
  });

  const genreData = getSortedData(stats.value.genres);
  new Chart(genreChartRef.value, { type: 'doughnut', data: { labels: genreData.labels, datasets: [{ data: genreData.data, backgroundColor: darkColors }] }, options: getPieOptions('genre') });

  const studioData = getSortedData(stats.value.studios);
  new Chart(studioChartRef.value, { type: 'bar', data: { labels: studioData.labels, datasets: [{ label: 'Anime Count', data: studioData.data, backgroundColor: 'rgba(163, 113, 247, 0.2)', borderColor: '#a371f7', borderWidth: 1 }] }, options: { indexAxis: 'y', onClick: handleChartClick('studio') } });

  const sortedYears = Object.keys(stats.value.years).sort();
  const yearVals = sortedYears.map(y => stats.value.years[y]);
  new Chart(yearChartRef.value, { type: 'bar', data: { labels: sortedYears, datasets: [{ label: 'Anime Count', data: yearVals, backgroundColor: 'rgba(88, 166, 255, 0.2)', borderColor: '#58a6ff', borderWidth: 1 }] }, options: { onClick: handleChartClick('year') } });

  const sourceData = getSortedData(stats.value.sources);
  new Chart(sourceChartRef.value, { type: 'pie', data: { labels: sourceData.labels, datasets: [{ data: sourceData.data, backgroundColor: darkColors }] }, options: getPieOptions('source') });

  const demoData = getSortedData(stats.value.demographics);
  new Chart(demoChartRef.value, { type: 'doughnut', data: { labels: demoData.labels, datasets: [{ data: demoData.data, backgroundColor: darkColors }] }, options: getPieOptions('demo') });

  new Chart(epChartRef.value, { type: 'bar', data: { labels: Object.keys(stats.value.ep_prefs), datasets: [{ label: 'Anime Count', data: Object.values(stats.value.ep_prefs), backgroundColor: 'rgba(63, 185, 80, 0.2)', borderColor: '#3fb950', borderWidth: 1 }] }, options: { onClick: handleChartClick('ep') } });
}
</script>

<template>
  <div v-if="!isLoading && stats" class="wrapper" style="max-width: 1200px; margin-top: 60px;">
      <h1 style="text-align: center; color: #c9d1d9;">MAL Data Analytics</h1>
      <p style="text-align: center; color: #8b949e; font-family: monospace; margin-bottom: 40px;">> ANALYSIS REPORT FOR: {{ userId }}</p>

      <div style="display: flex; justify-content: center; gap: 30px; margin-bottom: 30px; flex-wrap: wrap;">
          <div class="chart-card" style="width: 180px;">
              <div class="stat-box">{{ stats.total_watched.toLocaleString() }}</div>
              <div class="stat-label">Total Selected</div>
          </div>
          <div class="chart-card" style="width: 180px;">
              <div class="stat-box" style="color: #ffd700;">{{ stats.avg_score }}</div>
              <div class="stat-label">MAL Avg Score</div>
          </div>
          <div class="chart-card" style="width: 180px;">
              <div class="stat-box" style="color: #ff7b72;">{{ stats.total_hours.toLocaleString() }}</div>
              <div class="stat-label">Hours Watched</div>
          </div>
          <div class="chart-card" style="width: 180px;">
              <div class="stat-box" style="color: #3fb950;">{{ stats.total_eps.toLocaleString() }}</div>
              <div class="stat-label">Total Episodes</div>
          </div>
      </div>

      <div class="dashboard-grid">
          <div class="chart-card">
              <div class="click-hint">[CLICKABLE]</div>
              <h3 style="margin-top: 0;">Top Genres & Themes</h3>
              <canvas ref="genreChartRef"></canvas>
          </div>
          <div class="chart-card">
              <div class="click-hint">[CLICKABLE]</div>
              <h3 style="margin-top: 0;">Production Studios</h3>
              <canvas ref="studioChartRef"></canvas>
          </div>
          <div class="chart-card">
              <div class="click-hint">[CLICKABLE]</div>
              <h3 style="margin-top: 0;">Year Distribution</h3>
              <canvas ref="yearChartRef"></canvas>
          </div>
          <div class="chart-card">
              <div class="click-hint">[CLICKABLE]</div>
              <h3 style="margin-top: 0;">Source Material</h3>
              <canvas ref="sourceChartRef"></canvas>
          </div>
          <div class="chart-card">
              <div class="click-hint">[CLICKABLE]</div>
              <h3 style="margin-top: 0;">Target Demographics</h3>
              <canvas ref="demoChartRef"></canvas>
          </div>
          <div class="chart-card">
              <div class="click-hint">[CLICKABLE]</div>
              <h3 style="margin-top: 0;">Episode Preference</h3>
              <canvas ref="epChartRef"></canvas>
          </div>

          <div class="chart-card" style="grid-column: span 2;">
              <h3 style="margin-top: 0; color: #ffd700; text-align: left;">> Global Rank Archive</h3>
              <div class="horizontal-scroll-container">
                  <div v-for="(item, index) in stats.all_ranked" :key="'rank'+index" class="anime-card">
                      <img :src="item.img || 'https://cdn.myanimelist.net/img/sp/icon/apple-touch-icon-256.png'" alt="Cover">
                      <div class="anime-title" :title="item.title">{{item.title}}</div>
                      <div class="rank-badge" :class="{ 'badge-gold': index < 3, 'badge-red': index >= stats.all_ranked.length - 3 }">Rank #{{item.val.toLocaleString()}}</div>
                  </div>
              </div>
          </div>

          <div class="chart-card" style="grid-column: span 2;">
              <h3 style="margin-top: 0; color: #ff7b72; text-align: left;">> Popularity Archive</h3>
              <div class="horizontal-scroll-container">
                  <div v-for="(item, index) in stats.all_popular" :key="'pop'+index" class="anime-card">
                      <img :src="item.img || 'https://cdn.myanimelist.net/img/sp/icon/apple-touch-icon-256.png'" alt="Cover">
                      <div class="anime-title" :title="item.title">{{item.title}}</div>
                      <div class="rank-badge" :class="{ 'badge-gold': index < 3, 'badge-red': index >= stats.all_popular.length - 3 }">Pop #{{item.val.toLocaleString()}}</div>
                  </div>
              </div>
          </div>
      </div>
      
      <div style="text-align: center; margin-top: 40px;">
          <router-link :to="`/select/${userId}`" class="btn btn-secondary" style="font-family: monospace; text-decoration: none;">[ Return to Selection ]</router-link>
      </div>
  </div>

  <div v-show="showModal" class="modal-overlay" @click.self="showModal = false">
      <div class="modal-box">
          <div class="modal-header">
              <h2 style="margin: 0; color: #58a6ff; font-size: 18px;">{{ modalTitle }}</h2>
              <span class="close-btn" @click="showModal = false">&times;</span>
          </div>
          <div class="modal-body" ref="modalContentRef">
              <div v-for="item in modalItems" :key="item.mal_id || item.title" class="anime-card" style="width: 100%;">
                  <img :src="item.img_url || 'https://cdn.myanimelist.net/img/sp/icon/apple-touch-icon-256.png'" alt="Cover">
                  <div class="anime-title" :title="item.baha_title || item.title">{{ item.baha_title || item.title }}</div>
                  <div class="rank-badge">Rank {{ item.rank < 99999 ? '#' + item.rank.toLocaleString() : 'N/A' }}</div>
              </div>
          </div>
      </div>
  </div>
</template>

<style scoped>
.dashboard-grid { 
    display: grid; 
    grid-template-columns: 1fr 1fr; 
    gap: 20px; 
    width: 100%; 
    max-width: 1200px; 
    margin-top: 20px; 
}
.chart-card { 
    background: #0d1117; 
    border: 1px solid #30363d; 
    border-radius: 8px; 
    padding: 20px; 
    text-align: center; 
    position: relative;
}
.click-hint {
    font-size: 10px;
    color: #8b949e;
    position: absolute;
    top: 15px;
    right: 15px;
    font-family: monospace;
}
.stat-box { 
    font-size: 32px; 
    font-weight: bold; 
    color: #58a6ff; 
    font-family: monospace; 
}
.stat-label { 
    font-size: 12px; 
    color: #8b949e; 
    text-transform: uppercase; 
}
canvas { 
    max-height: 300px; 
    cursor: pointer;
}

.horizontal-scroll-container {
    display: flex;
    overflow-x: auto;
    gap: 15px;
    padding: 15px 5px;
    scrollbar-width: thin;
    scrollbar-color: #58a6ff #21262d;
}
.horizontal-scroll-container::-webkit-scrollbar { height: 10px; }
.horizontal-scroll-container::-webkit-scrollbar-track { background: #21262d; border-radius: 5px; }
.horizontal-scroll-container::-webkit-scrollbar-thumb { background-color: #58a6ff; border-radius: 5px; }

.anime-card {
    width: 130px; 
    flex: 0 0 130px;
    background: #010409;
    border: 1px solid #30363d;
    border-radius: 8px;
    padding: 10px;
    text-align: center;
    display: flex;
    flex-direction: column;
    align-items: center;
    transition: transform 0.2s;
    box-sizing: border-box; 
}
.anime-card:hover { transform: translateY(-5px); border-color: #58a6ff; }
.anime-card img {
    width: 100%; aspect-ratio: 3 / 4; height: auto;
    object-fit: cover; object-position: center;
    border-radius: 4px; margin-bottom: 10px; border: 1px solid #21262d;
}
.anime-card .anime-title {
    font-size: 12px; color: #c9d1d9; width: 100%;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
    margin-bottom: 5px; font-weight: bold;
}
.anime-card .rank-badge {
    font-size: 11px; padding: 3px 8px; border-radius: 10px;
    background: #21262d; color: #8b949e; font-weight: bold; width: 80%;
}
.badge-gold { color: #ffd700; background: rgba(255,215,0,0.1); border: 1px solid rgba(255,215,0,0.3); }
.badge-red { color: #ff7b72; background: rgba(255,123,114,0.1); border: 1px solid rgba(255,123,114,0.3); }

.modal-overlay {
    position: fixed; top: 0; left: 0; width: 100%; height: 100%;
    background: rgba(1, 4, 9, 0.85); z-index: 1000; display: flex; justify-content: center; align-items: center;
    backdrop-filter: blur(4px);
}
.modal-box {
    background: #0d1117; border: 1px solid #30363d; border-radius: 8px;
    width: 90%; max-width: 1000px; max-height: 85vh; display: flex; flex-direction: column;
    box-shadow: 0 8px 24px rgba(0,0,0,0.8);
}
.modal-header {
    padding: 20px; border-bottom: 1px solid #30363d; display: flex; justify-content: space-between; align-items: center;
}
.modal-body {
    padding: 20px; overflow-y: auto; display: grid;
    grid-template-columns: repeat(auto-fill, minmax(130px, 1fr)); gap: 15px;
    scrollbar-width: thin; scrollbar-color: #58a6ff #21262d;
}
.close-btn { color: #8b949e; font-size: 28px; cursor: pointer; transition: 0.2s; line-height: 1; }
.close-btn:hover { color: #ff7b72; }
</style>
