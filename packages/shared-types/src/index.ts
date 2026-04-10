// Varro Shared Types
// These types are used across web and worker apps

export interface Well {
  id: string
  apiNumber: string // 14-digit API number (canonical key)
  operator: string
  operatorNormalised: string
  wellName: string
  basin: 'GOM' | 'ONSHORE' | 'AK' | 'PACIFIC'
  state: string
  county?: string
  latitude?: number
  longitude?: number
  wellType: WellType
  wellStatus: WellStatus
  spudDate?: string
  totalDepthFt?: number
  waterDepthFt?: number
  isDelinquent: boolean
  delinquencyDate?: string
  createdAt: string
  updatedAt: string
}

export type WellType = 'OIL' | 'GAS' | 'DRY' | 'INJECTION' | 'DISPOSAL' | 'OBSERVATION' | 'OTHER'

export type WellStatus = 
  | 'ACTIVE'
  | 'IDLE'
  | 'TEMP_ABANDONED'
  | 'PERMANENTLY_ABANDONED'
  | 'PLUGGED'
  | 'PLUGGED_ABANDONED'
  | 'UNKNOWN'

export interface AROEstimate {
  id: string
  wellId: string
  wellApiNumber: string
  estimateP50Usd: number
  estimateP90Usd: number
  modelVersion: string
  comparableCount: number
  methodologyNotes: string
  citationUrls: string[]
  createdAt: string
}

export interface IngestionRun {
  id: string
  source: DataSource
  status: 'PENDING' | 'RUNNING' | 'COMPLETED' | 'FAILED'
  recordsProcessed: number
  recordsFailed: number
  startedAt: string
  completedAt?: string
  errorMessage?: string
}

export type DataSource = 
  | 'BSEE_API' 
  | 'BOEM_LEASES' 
  | 'BOEM_PA_COSTS' 
  | 'IOGCC' 
  | 'EPA_ECHO'

export interface Operator {
  id: string
  canonicalName: string
  aliases: string[]
  boeoCode?: string // BOEM operator code
  bseeCode?: string
  wellCount: number
  delinquentWellCount: number
  estimatedAroExposureUsd?: number
}
