'use client'

import { useEffect, useState } from 'react'
import { Loader2, CheckCircle2, XCircle } from 'lucide-react'
import { getStatus } from '@/services/api'
import { ProcessingStatus as Status } from '@/types'
import { POLL_INTERVAL } from '@/utils/constants'

interface ProcessingStatusProps {
  uploadId: number
  onStatusUpdate: (status: string) => void
}

export default function ProcessingStatus({ uploadId, onStatusUpdate }: ProcessingStatusProps) {
  const [progress, setProgress] = useState(0)
  const [status, setStatus] = useState<string>('processing')
  const [message, setMessage] = useState('Initializing...')

  useEffect(() => {
    let intervalId: NodeJS.Timeout

    const pollStatus = async () => {
      try {
        const response = await getStatus(uploadId)
        
        if (response.success && response.data) {
          const { status: newStatus, progress: newProgress } = response.data
          
          setStatus(newStatus)
          setProgress(newProgress || 0)
          
          // Update message based on progress
          if (newProgress === 25) setMessage('Reading Excel file...')
          else if (newProgress === 50) setMessage('Classifying transactions...')
          else if (newProgress === 75) setMessage('Validating data...')
          else if (newProgress === 100) setMessage('Processing complete!')
          
          // Update parent component
          if (newStatus === Status.COMPLETED || newStatus === Status.FAILED) {
            onStatusUpdate(newStatus)
            clearInterval(intervalId)
          }
        }
      } catch (error) {
        console.error('Error polling status:', error)
      }
    }

    // Poll immediately
    pollStatus()

    // Then poll every 2 seconds
    intervalId = setInterval(pollStatus, POLL_INTERVAL)

    return () => clearInterval(intervalId)
  }, [uploadId, onStatusUpdate])

  return (
    <div className="w-full max-w-2xl mx-auto">
      <div className="text-center mb-8">
        <div className="flex justify-center mb-4">
          {status === Status.PROCESSING && (
            <Loader2 className="w-16 h-16 text-primary-600 animate-spin" />
          )}
          {status === Status.COMPLETED && (
            <CheckCircle2 className="w-16 h-16 text-green-600" />
          )}
          {status === Status.FAILED && (
            <XCircle className="w-16 h-16 text-red-600" />
          )}
        </div>

        <h2 className="text-2xl font-bold text-gray-900 mb-2">
          {status === Status.PROCESSING && 'Processing Your File'}
          {status === Status.COMPLETED && 'Processing Complete!'}
          {status === Status.FAILED && 'Processing Failed'}
        </h2>

        <p className="text-gray-600">{message}</p>
      </div>

      {/* Progress Bar */}
      <div className="w-full bg-gray-200 rounded-full h-3 mb-4 overflow-hidden">
        <div
          className="bg-gradient-to-r from-primary-500 to-primary-600 h-3 rounded-full transition-all duration-500 ease-out"
          style={{ width: `${progress}%` }}
        ></div>
      </div>

      <div className="text-center text-sm text-gray-500">
        {progress}% Complete
      </div>

      {/* Processing Steps */}
      <div className="mt-8 space-y-3">
        <ProcessingStep
          title="Reading file"
          completed={progress >= 25}
          active={progress < 25}
        />
        <ProcessingStep
          title="Classifying transactions"
          completed={progress >= 50}
          active={progress >= 25 && progress < 50}
        />
        <ProcessingStep
          title="Validating data"
          completed={progress >= 75}
          active={progress >= 50 && progress < 75}
        />
        <ProcessingStep
          title="Generating template"
          completed={progress === 100}
          active={progress >= 75 && progress < 100}
        />
      </div>
    </div>
  )
}

function ProcessingStep({
  title,
  completed,
  active,
}: {
  title: string
  completed: boolean
  active: boolean
}) {
  return (
    <div className="flex items-center space-x-3">
      <div
        className={`
          w-6 h-6 rounded-full flex items-center justify-center
          ${completed ? 'bg-green-500' : active ? 'bg-primary-500' : 'bg-gray-300'}
        `}
      >
        {completed && (
          <CheckCircle2 className="w-4 h-4 text-white" />
        )}
        {active && !completed && (
          <Loader2 className="w-4 h-4 text-white animate-spin" />
        )}
      </div>
      <span
        className={`
          text-sm
          ${completed ? 'text-gray-900 font-medium' : active ? 'text-gray-700' : 'text-gray-400'}
        `}
      >
        {title}
      </span>
    </div>
  )
}
