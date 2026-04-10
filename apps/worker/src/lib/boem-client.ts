/**
 * BOEM API Client — Lease & Delinquency Data
 * Source: https://www.data.boem.gov/
 * PRO-181: BOEM API client + lease/delinquency normalizer
 */

import axios, { AxiosInstance } from 'axios'
import pino from 'pino'

const logger = pino({ level: process.env.LOG_LEVEL || 'info' })

// BOEM data endpoints (public OData/REST)
const BOEM_BASE = 'https://www.data.boem.gov/api'

export interface BOEMLease {
  leaseNumber: string
  operatorName: string
  block: string
  area: string
  leaseStatus: string    // 'PROD' | 'UNIT' | 'SOP' | 'CANC' | etc.
  issuedDate?: string
  expirationDate?: string
  termDate?: string
}

export interface BOEMDelinquentWell {
  apiNumber: string
  wellName: string
  operatorName: string
  leaseNumber: string
  delinquencyDate: string
  delinquencyType: string  // 'P&A' | 'Inspection' | etc.
}

export class BOEMClient {
  private client: AxiosInstance

  constructor() {
    this.client = axios.create({
      baseURL: BOEM_BASE,
      timeout: 30_000,
      headers: {
        'Accept': 'application/json',
        'User-Agent': 'Varro/0.1 (contact@varro.io)',
      },
    })
  }

  /**
   * Fetch delinquent well list from BOEM TIMS
   * Returns wells that have missed P&A deadlines
   */
  async fetchDelinquentWells(): Promise<BOEMDelinquentWell[]> {
    logger.info('Fetching BOEM delinquent wells')
    try {
      // BOEM TIMS delinquency endpoint (exact path TBD — verify at data.boem.gov)
      const response = await this.client.get('/Well/GetDelinquentWellList', {
        params: { $format: 'json', $top: 5000 },
      })
      return response.data?.value ?? []
    } catch (err) {
      logger.error({ err }, 'Failed to fetch BOEM delinquent wells — endpoint may need verification')
      return []
    }
  }

  /**
   * Fetch GOM lease inventory
   */
  async fetchLeases(page = 0, pageSize = 500): Promise<BOEMLease[]> {
    const skip = page * pageSize
    logger.info({ page, skip }, 'Fetching BOEM leases')
    try {
      const response = await this.client.get('/Lease/GetLeaseList', {
        params: {
          $format: 'json',
          $skip: skip,
          $top: pageSize,
        },
      })
      return response.data?.value ?? []
    } catch (err) {
      logger.error({ err }, 'Failed to fetch BOEM leases')
      return []
    }
  }
}
