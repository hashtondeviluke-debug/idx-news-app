import { ExternalLink, Sparkles, AlertCircle } from 'lucide-react'
import { cn } from '../lib/utils'
import type { NewsArticle } from '../types/news'
import { getSentimentLevel } from '../types/news'
import { SentimentBadge } from './SentimentBadge'

/**
 * Sanitize a URL before putting it in an href:
 * - Strip whitespace
 * - Collapse double-slashes in path (keep https://)
 * - Resolve protocol-relative //example.com → https://example.com
 * - Prefix scheme-less URLs → https://...
 * - Block javascript: / data: schemes
 */
function sanitizeUrl(raw: string): string {
  const url = (raw ?? '').trim()
  if (!url) return '#'
  if (/^javascript:/i.test(url) || /^data:/i.test(url)) return '#'
  // Already absolute http(s) — collapse double slashes in path only
  if (/^https?:\/\//i.test(url)) return url.replace(/([^:])\/\/+/g, '$1/')
  // Protocol-relative
  if (url.startsWith('//')) return 'https:' + url
  // Root-relative (keep as-is — scraper should have resolved these already)
  if (url.startsWith('/')) return url
  // No scheme
  return 'https://' + url
}

const CATEGORY_COLORS: Record<string, string> = {
  'Corporate Action': 'bg-violet-500/10 text-violet-400 border-violet-500/20',
  'Financial Result': 'bg-blue-500/10 text-blue-400 border-blue-500/20',
  'Macro Economy':    'bg-orange-500/10 text-orange-400 border-orange-500/20',
  'Regulatory':       'bg-yellow-500/10 text-yellow-400 border-yellow-500/20',
  'Market Movement':  'bg-cyan-500/10 text-cyan-400 border-cyan-500/20',
  'Other':            'bg-muted/30 text-muted-foreground border-border',
}

function timeAgo(iso: string): string {
  const diff = Date.now() - new Date(iso).getTime()
  const h = Math.floor(diff / 3.6e6)
  const m = Math.floor(diff / 60000)
  if (h > 23) return `${Math.floor(h / 24)}d ago`
  if (h >= 1) return `${h}h ago`
  if (m >= 1) return `${m}m ago`
  return 'just now'
}

interface Props {
  article: NewsArticle
  index?: number
}

export function NewsCard({ article, index = 0 }: Props) {
  const analysis  = article.ai_analysis
  const level     = analysis ? getSentimentLevel(analysis.impact_score) : null
  const safeUrl   = sanitizeUrl(article.url)
  const isValid   = safeUrl !== '#' && /^https?:\/\//i.test(safeUrl)

  return (
    <article
      className={cn(
        'group relative rounded-lg border bg-card p-4 transition-all duration-200',
        'hover:shadow-md hover:border-border/80',
        'animate-slide-up',
        level === 'bullish' && 'hover:border-emerald-500/30',
        level === 'bearish' && 'hover:border-red-500/30',
        level === 'neutral' && 'hover:border-amber-500/20',
      )}
      style={{ animationDelay: `${Math.min(index * 40, 400)}ms` }}
    >
      {/* left sentinel bar */}
      <div
        className={cn(
          'absolute left-0 top-3 bottom-3 w-0.5 rounded-full transition-opacity opacity-0 group-hover:opacity-100',
          level === 'bullish' && 'bg-emerald-500',
          level === 'bearish' && 'bg-red-500',
          level === 'neutral' && 'bg-amber-500',
          !level && 'bg-muted-foreground'
        )}
      />

      {/* header row */}
      <div className="flex items-start justify-between gap-3 mb-3">
        <div className="flex flex-wrap items-center gap-1.5 min-w-0">
          <span className="text-xs font-terminal font-medium text-muted-foreground bg-muted/50 px-2 py-0.5 rounded border border-border/50">
            {article.source}
          </span>

          {analysis && (
            <span
              className={cn(
                'text-xs font-medium px-2 py-0.5 rounded border',
                CATEGORY_COLORS[analysis.category] ?? CATEGORY_COLORS['Other']
              )}
            >
              {analysis.category}
            </span>
          )}

          <span className="text-xs font-terminal text-muted-foreground/60 ml-auto">
            {timeAgo(article.scraped_at)}
          </span>
        </div>

        {analysis ? (
          <div className="flex-shrink-0">
            <SentimentBadge score={analysis.impact_score} label={analysis.impact_label} size="sm" />
          </div>
        ) : (
          <AlertCircle size={16} className="text-muted-foreground flex-shrink-0 mt-0.5" />
        )}
      </div>

      {/* title — primary click target */}
      <a
        href={safeUrl}
        target="_blank"
        rel="noopener noreferrer"
        className="group/link block mb-2"
        title={isValid ? safeUrl : 'Article URL unavailable'}
        onClick={(e) => { if (!isValid) e.preventDefault() }}
      >
        <h3 className="text-sm font-semibold leading-snug text-foreground group-hover/link:text-primary transition-colors line-clamp-2">
          {article.title}
          <ExternalLink
            size={11}
            className="inline ml-1.5 opacity-0 group-hover/link:opacity-60 transition-opacity"
          />
        </h3>
      </a>

      {/* AI summary */}
      {analysis?.summary ? (
        <div className="mb-3">
          <div className="flex items-center gap-1.5 mb-1.5">
            <Sparkles size={11} className="text-primary flex-shrink-0" />
            <span className="text-xs font-medium text-primary/80">AI Analysis</span>
            <span
              className={cn(
                'text-xs font-terminal ml-auto',
                analysis.confidence === 'high'   && 'text-emerald-500/70',
                analysis.confidence === 'medium' && 'text-amber-500/70',
                analysis.confidence === 'low'    && 'text-muted-foreground/60'
              )}
            >
              {analysis.confidence} confidence
            </span>
          </div>
          <p className="text-xs text-muted-foreground leading-relaxed">
            {analysis.summary}
          </p>
        </div>
      ) : (
        <p className="text-xs text-muted-foreground/50 italic mb-3">
          AI analysis not available for this article.
        </p>
      )}

      {/* footer: emiten chips + Read Article button */}
      <div className="flex items-center justify-between gap-2 flex-wrap mt-1">
        <div className="flex flex-wrap gap-1">
          {analysis?.target_emiten.map((code) => (
            <span
              key={code}
              className="font-terminal text-xs font-semibold px-1.5 py-0.5 rounded bg-primary/10 text-primary border border-primary/20"
            >
              {code}
            </span>
          ))}
        </div>

        {/* explicit Read Article button — secondary affordance */}
        <a
          href={safeUrl}
          target="_blank"
          rel="noopener noreferrer"
          onClick={(e) => { if (!isValid) e.preventDefault() }}
          title={isValid ? safeUrl : 'URL unavailable'}
          className={cn(
            'flex-shrink-0 flex items-center gap-1 text-xs font-medium px-2.5 py-1 rounded border transition-colors',
            isValid
              ? 'border-border text-muted-foreground hover:text-primary hover:border-primary/30 hover:bg-primary/5'
              : 'border-border/30 text-muted-foreground/30 cursor-not-allowed pointer-events-none'
          )}
        >
          <ExternalLink size={10} />
          Read Article
        </a>
      </div>
    </article>
  )
}
