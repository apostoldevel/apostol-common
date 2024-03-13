/*++

Program name:

  Apostol CRM

Module Name:

  FetchCommon.cpp

Notices:

  Module: Postgres Fetch Common

Author:

  Copyright (c) Prepodobny Alen

  mailto: alienufo@inbox.ru
  mailto: ufocomp@gmail.com

--*/

//----------------------------------------------------------------------------------------------------------------------

#include "Core.hpp"
#include "FetchCommon.hpp"
//----------------------------------------------------------------------------------------------------------------------

#define FETCH_TIMEOUT_INTERVAL 60000
//----------------------------------------------------------------------------------------------------------------------

extern "C++" {

namespace Apostol {

    namespace Module {

        //--------------------------------------------------------------------------------------------------------------

        //-- CFetchHandler ---------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        CFetchHandler::CFetchHandler(CQueueCollection *ACollection, const CString &RequestId, COnQueueHandlerEvent && Handler):
                CQueueHandler(ACollection, static_cast<COnQueueHandlerEvent &&> (Handler)) {

            m_TimeOut = INFINITE;
            m_TimeOutInterval = FETCH_TIMEOUT_INTERVAL;

            m_RequestId = RequestId;
        }

        //--------------------------------------------------------------------------------------------------------------

        //-- CFetchCommon ----------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        CFetchCommon::CFetchCommon(CModuleProcess *AProcess, const CString &ModuleName, const CString &SectionName):
                CQueueCollection(Config()->PostgresPollMin()), CApostolModule(AProcess, ModuleName, SectionName) {

            m_Headers.Add("Authorization");

            m_Progress = 0;
            m_TimeOut = 0;
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFetchCommon::QueryException(CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
            auto pConnection = dynamic_cast<CHTTPServerConnection *> (APollQuery->Binding());
            ReplyError(pConnection, CHTTPReply::internal_server_error, E.what());
        }
        //--------------------------------------------------------------------------------------------------------------

        int CFetchCommon::CheckError(const CJSON &Json, CString &ErrorMessage) {
            int errorCode = 0;

            if (Json.HasOwnProperty(_T("error"))) {
                const auto& error = Json[_T("error")];

                if (error.HasOwnProperty(_T("code"))) {
                    errorCode = error[_T("code")].AsInteger();
                } else {
                    return 0;
                }

                if (error.HasOwnProperty(_T("message"))) {
                    ErrorMessage = error[_T("message")].AsString();
                } else {
                    return 0;
                }

                if (errorCode >= 10000)
                    errorCode = errorCode / 100;

                if (errorCode < 0)
                    errorCode = 400;
            }

            return errorCode;
        }
        //--------------------------------------------------------------------------------------------------------------

        CHTTPReply::CStatusType CFetchCommon::ErrorCodeToStatus(int ErrorCode) {
            CHTTPReply::CStatusType status = CHTTPReply::ok;

            if (ErrorCode != 0) {
                switch (ErrorCode) {
                    case 401:
                        status = CHTTPReply::unauthorized;
                        break;

                    case 403:
                        status = CHTTPReply::forbidden;
                        break;

                    case 404:
                        status = CHTTPReply::not_found;
                        break;

                    case 500:
                        status = CHTTPReply::internal_server_error;
                        break;

                    default:
                        status = CHTTPReply::bad_request;
                        break;
                }
            }

            return status;
        }
        //--------------------------------------------------------------------------------------------------------------

        CJSON CFetchCommon::ParamsToJson(const CStringList &Params) {
            CJSON Json;
            for (int i = 0; i < Params.Count(); i++) {
                Json.Object().AddPair(Params.Names(i), Params.Values(i));
            }
            return Json;
        }
        //--------------------------------------------------------------------------------------------------------------

        CJSON CFetchCommon::HeadersToJson(const CHeaders &Headers) {
            CJSON Json;
            for (int i = 0; i < Headers.Count(); i++) {
                const auto &caHeader = Headers[i];
                Json.Object().AddPair(caHeader.Name(), caHeader.Value());
            }
            return Json;
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFetchCommon::DoError(const Delphi::Exception::Exception &E) {
            Log()->Error(APP_LOG_ERR, 0, "[%s] Error: %s", ModuleName().c_str(), E.what());
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFetchCommon::DeleteHandler(CQueueHandler *AHandler) {
            auto pHandler = dynamic_cast<CFetchHandler *> (AHandler);
            if (Assigned(pHandler)) {
                CQueueCollection::DeleteHandler(AHandler);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFetchCommon::DoConnected(CObject *Sender) {
            auto pConnection = dynamic_cast<CHTTPClientConnection *>(Sender);
            if (Assigned(pConnection)) {
                auto pSocket = pConnection->Socket();
                if (pSocket != nullptr) {
                    auto pHandle = pSocket->Binding();
                    if (pHandle != nullptr) {
                        Log()->Notice(_T("[%s] [%s:%d] Client connected."), ModuleName().c_str(), pHandle->PeerIP(), pHandle->PeerPort());
                    }
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFetchCommon::DoDisconnected(CObject *Sender) {
            auto pConnection = dynamic_cast<CHTTPClientConnection *>(Sender);
            if (Assigned(pConnection)) {
                auto pSocket = pConnection->Socket();
                if (pSocket != nullptr) {
                    auto pHandle = pSocket->Binding();
                    if (pHandle != nullptr) {
                        Log()->Notice(_T("[%s] [%s:%d] Client disconnected."), ModuleName().c_str(), pHandle->PeerIP(), pHandle->PeerPort());
                    }
                } else {
                    Log()->Notice(_T("[%s] Client disconnected."), ModuleName().c_str());
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFetchCommon::DoPostgresQueryExecuted(CPQPollQuery *APollQuery) {

            auto pResult = APollQuery->Results(0);

            try {
                if (pResult->ExecStatus() != PGRES_TUPLES_OK)
                    throw Delphi::Exception::EDBError(pResult->GetErrorMessage());

                CString errorMessage;

                auto pConnection = dynamic_cast<CHTTPServerConnection *> (APollQuery->Binding());

                if (pConnection != nullptr && !pConnection->ClosedGracefully()) {
                    const auto &caRequest = pConnection->Request();
                    auto &Reply = pConnection->Reply();

                    CStringList ResultObject;
                    CStringList ResultFormat;

                    ResultObject.Add("true");
                    ResultObject.Add("false");

                    ResultFormat.Add("object");
                    ResultFormat.Add("array");
                    ResultFormat.Add("null");

                    const auto &result_object = caRequest.Params[_T("result_object")];
                    const auto &result_format = caRequest.Params[_T("result_format")];

                    if (!result_object.IsEmpty() && ResultObject.IndexOfName(result_object) == -1) {
                        ReplyError(pConnection, CHTTPReply::bad_request, CString().Format("Invalid result_object: %s", result_object.c_str()));
                        return;
                    }

                    if (!result_format.IsEmpty() && ResultFormat.IndexOfName(result_format) == -1) {
                        ReplyError(pConnection, CHTTPReply::bad_request, CString().Format("Invalid result_format: %s", result_format.c_str()));
                        return;
                    }

                    CHTTPReply::CStatusType status = CHTTPReply::ok;

                    try {
                        if (pResult->nTuples() == 1) {
                            const CJSON Payload(pResult->GetValue(0, 0));
                            status = ErrorCodeToStatus(CheckError(Payload, errorMessage));
                        }

                        PQResultToJson(pResult, Reply.Content, result_format, result_object == "true" ? "result" : CString());
                    } catch (Delphi::Exception::Exception &E) {
                        errorMessage = E.what();
                        status = CHTTPReply::bad_request;
                        Log()->Error(APP_LOG_ERR, 0, "%s", E.what());
                    }

                    if (status == CHTTPReply::ok) {
                        pConnection->SendReply(status, nullptr, true);
                    } else {
                        ReplyError(pConnection, status, errorMessage);
                    }
                }
            } catch (Delphi::Exception::Exception &E) {
                QueryException(APollQuery, E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFetchCommon::DoPostgresQueryException(CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
            QueryException(APollQuery, E);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFetchCommon::DoDone(CFetchHandler *AHandler, const CHTTPReply &Reply) {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {
                auto pHandler = dynamic_cast<CFetchHandler *> (APollQuery->Binding());
                DeleteHandler(pHandler);
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                auto pHandler = dynamic_cast<CFetchHandler *> (APollQuery->Binding());
                DeleteHandler(pHandler);
                DoError(E);
            };

            const auto &caPayload = AHandler->Payload();

            const auto &caHeaders = PQQuoteLiteral(HeadersToJson(Reply.Headers).ToString());
            const auto &caContent = PQQuoteLiteral(base64_encode(Reply.Content));

            const auto &caRequest = caPayload["id"].AsString();
            const auto &caDone = caPayload["done"];

            CStringList SQL;

            SQL.Add(CString()
                            .MaxFormatSize(256 + caRequest.Size() + caHeaders.Size() + caContent.Size())
                            .Format("SELECT http.create_response(%s::uuid, %d, %s, %s::jsonb, decode(%s, 'base64'));",
                                    PQQuoteLiteral(caRequest).c_str(),
                                    (int) Reply.Status,
                                    PQQuoteLiteral(Reply.StatusText).c_str(),
                                    caHeaders.c_str(),
                                    caContent.c_str()
                            ));

            if (!caDone.IsNull()) {
                SQL.Add(CString().Format("SELECT %s(%s::uuid);", caDone.AsString().c_str(), PQQuoteLiteral(caRequest).c_str()));
            }

            try {
                ExecSQL(SQL, AHandler, OnExecuted, OnException);
            } catch (Delphi::Exception::Exception &E) {
                DeleteHandler(AHandler);
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFetchCommon::DoFail(CFetchHandler *AHandler, const CString &Message) {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {
                auto pHandler = dynamic_cast<CFetchHandler *> (APollQuery->Binding());
                DeleteHandler(pHandler);
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                auto pHandler = dynamic_cast<CFetchHandler *> (APollQuery->Binding());
                DeleteHandler(pHandler);
                DoError(E);
            };

            const auto &caPayload = AHandler->Payload();
            const auto &caRequest = caPayload["id"].AsString();
            const auto &caFail = caPayload["fail"];

            CStringList SQL;

            SQL.Add(CString()
                            .MaxFormatSize(256 + caRequest.Size() + Message.Size())
                            .Format("SELECT http.fail(%s::uuid, %s);",
                                    PQQuoteLiteral(caRequest).c_str(),
                                    PQQuoteLiteral(Message).c_str()
                            ));

            if (!caFail.IsNull()) {
                SQL.Add(CString().Format("SELECT %s(%s::uuid);", caFail.AsString().c_str(), PQQuoteLiteral(caRequest).c_str()));
            }

            try {
                ExecSQL(SQL, AHandler, OnExecuted, OnException);
            } catch (Delphi::Exception::Exception &E) {
                DeleteHandler(AHandler);
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFetchCommon::DoStream(CFetchHandler *AHandler, const CString &Data) {

            auto OnExecuted = [](CPQPollQuery *APollQuery) {

            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                DoError(E);
            };

            const auto &caPayload = AHandler->Payload();
            const auto &caRequest = caPayload["id"].AsString();
            const auto &caStream = caPayload["stream"].AsString();

            if (caStream.IsEmpty())
                return;

            CStringList SQL;

            SQL.Add(CString()
                            .MaxFormatSize(256 + caRequest.Size() + Data.Size())
                            .Format("SELECT %s(%s::uuid, %s);",
                                    caStream.c_str(),
                                    PQQuoteLiteral(caRequest).c_str(),
                                    PQQuoteLiteral(Data).c_str()
                            ));

            try {
                ExecSQL(SQL, nullptr, OnExecuted, OnException);
            } catch (Delphi::Exception::Exception &E) {
                DeleteHandler(AHandler);
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFetchCommon::UnloadQueue() {
            const auto index = m_Queue.IndexOf(this);
            if (index != -1) {
                const auto pQueue = m_Queue[index];
                for (int i = 0; i < pQueue->Count(); ++i) {
                    auto pHandler = (CFetchHandler *) pQueue->Item(i);
                    if (pHandler != nullptr && pHandler->Allow()) {
                        pHandler->Handler();
                        if (m_Progress >= m_MaxQueue) {
                            Log()->Warning("[%s] [%d] [%d] Queued is full.", ModuleName().c_str(), m_Progress, m_MaxQueue);
                            break;
                        }
                    }
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFetchCommon::CheckTimeOut(CDateTime Now) {
            const auto index = m_Queue.IndexOf(this);
            if (index != -1) {
                const auto pQueue = m_Queue[index];
                for (int i = pQueue->Count() - 1; i >= 0; i--) {
                    auto pHandler = (CFetchHandler *) pQueue->Item(i);
                    if (pHandler != nullptr && !pHandler->Allow()) {
                        if ((pHandler->TimeOut() != INFINITE) && (Now >= pHandler->TimeOut())) {
                            DoFail(pHandler, "Connection timed out");
                        }
                    }
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

    }
}
}