/**
 * BSEE API Client
 * Source: https://www.data.bsee.gov/Main/API.aspx
 * Documentation: https://www.data.bsee.gov/Main/API.aspx
 * PRO-180: BSEE API client + GOM well normalizer
 */

import axios, { AxiosInstance } from 'axios'
import pino from 'pino'
import { Well, WellType, WellStatus } from '@varro/shared-types'

const logger = pino({ level: process.env.LOG_LEVEL || 'info' })

// BSEE OData API base
const BSEE_API_BASE = 'https://www.data.bsee.gov/api/Well/GetWellList'

// BSEE well record (raw from API)
interface BSEEWellRecord {
  WellBoreID: string
  APIWellNumber: string
  OperatorName: string
  WellBoreName: string
  SurfaceHoleBlock?: string
  SurfaceHoleArea?: string
  BottomHoleBlock?: string
  BottomHoleArea?: string
  WellTypeCode: string
  WellStatusCode: string
  SpudDate?: string
  TotalDepth?: number
  WaterDepth?: number
  Latitude?: number
  Longitude?: number
  StateCode?: string
}

// BSEE well type code → Varro WellType
const WELL_TYPE_MAP: Record<string, WellType> = {
  'OIL': 'OIL',
  'GAS': 'GAS',
  'D': 'DRY',
  'I': 'INJECTION',
  'SWD': 'DISPOSAL',
  'OBS': 'OBSERVATION',
}

// BSEE well status code → Varro WellStatus
const WELL_STATUS_MAP: Record<string, WellStatus> = {
  'AC': 'ACTIVE',
  'ID': 'IDLE',
  'TA': 'TEMP_ABANDONED',
  'PA': 'PERMANENTLY_ABANDONED',
  'P': 'PLUGGED',
  'PL': 'PLUGGED_ABANDONED',
}

export class BSEEClient {
  private client: AxiosInstance

  constructor() {
    this.client = axios.create({
      baseURL: BSEE_API_BASE,
      timeout: 30_000,
      headers: {
        'Accept': 'application/json',
        'User-Agent': 'Varro/0.1 (contact@varro.io)',
      },
    })
  }

  /**
   * Fetch a page of GOM well records from BSEE
   * @param page - 0-indexed page number
   * @param pageSize - records per page (max 1000)
   */
  async fetchWells(page = 0, pageSize = 500): Promise<BSEEWellRecord[]> {
    const skip = page * pageSize
    logger.info({ page, pageSize, skip }, 'Fetching BSEE wells')

    const response = await this.client.get('', {
      params: {
        $format: 'json',
        $skip: skip,
        $top: pageSize,
        $filter: "WaterDepth gt 0",  // GOM offshore wells only
      },
    })

    return response.data?.value ?? []
  }

  /**
   * Normalize a BSEE well record into a Varro Well shape
   */
  normalizeWell(raw: BSEEWellRecord): Partial<Well> {
    return {
      apiNumber: raw.APIWellNumber?.replace(/\D/g, '').padStart(14, '0').slice(0, 14),
      operatorRaw: raw.OperatorName,
      wellName: raw.WellBoreName,
      basin: 'GOM',
      wellType: WELL_TYPE_MAP[raw.WellTypeCode] ?? 'OTHER',
      wellStatus: WELL_STATUS_MAP[raw.WellStatusCode] ?? 'UNKNOWN',
      spudDate: raw.SpudDate?.split('T')[0],
      totalDepthFt: raw.TotalDepth ?? undefined,
      waterDepthFt: raw.WaterDepth ?? undefined,
      latitude: raw.Latitude ?? undefined,
      longitude: raw.Longitude ?? undefined,
      bseeLastSeen: new Date().toISOString(),
    }
  }

  /**
   * Fetch all GOM wells, paginated (yields batches)
   */
  async *fetchAllWells(pageSize = 500): AsyncGenerator<Partial<Well>[]> {
    let page = 0
    let fetched = pageSize

    while (fetched === pageSize) {
      const records = await this.fetchWells(page, pageSize)
      fetched = records.length
      logger.info({ page, fetched }, 'BSEE page fetched')
      yield records.map(r => this.normalizeWell(r))
      page++
      
      // Rate limit: 100ms between requests
      await new Promise(r => setTimeout(r, 100))
    }
    logger.info({ totalPages: page }, 'BSEE ingestion complete')
  }
}
