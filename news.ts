export interface AiAnalysis {
  summary: string
  impact_score: 1 | 2 | 3 | 4 | 5
  impact_label: 'Very Bearish' | 'Bearish' | 'Neutral' | 'Bullish' | 'Very Bullish'
  category:
    | 'Corporate Action'
    | 'Financial Result'
    | 'Macro Economy'
    | 'Regulatory'
    | 'Market Movement'
    | 'Other'
  target_emiten: string[]
  confidence: 'high' | 'medium' | 'low'
}

export interface NewsArticle {
  source: string
  title: string
  url: string
  body: string
  body_chars: number
  scraped_at: string
  ai_analysis: AiAnalysis | null
  ai_model?: string
  analyzed_at?: string
  ai_error?: string
}

export interface NewsSource {
  name: string
  base_url: string
  listing_url: string
  enabled: boolean
  request_delay_seconds: number
  article_list_selector: string
  title_selector: string
  link_selector: string
  link_prefix: string
  body_selectors: string[]
  notes?: string
}

export interface SourcesConfig {
  keywords: string[]
  sources: NewsSource[]
}

export type SentimentLevel = 'bullish' | 'neutral' | 'bearish'
export type Category = AiAnalysis['category'] | 'All'

export function getSentimentLevel(score: number): SentimentLevel {
  if (score >= 4) return 'bullish'
  if (score <= 2) return 'bearish'
  return 'neutral'
}

export function getSentimentColor(score: number) {
  if (score >= 4) return 'text-emerald-500 dark:text-emerald-400'
  if (score <= 2) return 'text-red-500 dark:text-red-400'
  return 'text-amber-500 dark:text-amber-400'
}

export function getSentimentBg(score: number) {
  if (score >= 4) return 'sentiment-bullish-bg'
  if (score <= 2) return 'sentiment-bearish-bg'
  return 'sentiment-neutral-bg'
}

export function getSentimentBadgeClass(score: number) {
  if (score >= 4) return 'score-badge sentiment-bullish sentiment-bullish-bg'
  if (score <= 2) return 'score-badge sentiment-bearish sentiment-bearish-bg'
  return 'score-badge sentiment-neutral sentiment-neutral-bg'
}
