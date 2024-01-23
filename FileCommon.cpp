/*++

Program name:

  Apostol CRM

Module Name:

  FileCommon.cpp

Notices:

  Module: File Common

Author:

  Copyright (c) Prepodobny Alen

  mailto: alienufo@inbox.ru
  mailto: ufocomp@gmail.com

--*/

//----------------------------------------------------------------------------------------------------------------------

#include "Core.hpp"
#include "FileCommon.hpp"
//----------------------------------------------------------------------------------------------------------------------

#define API_BOT_USERNAME "apibot"

#define QUERY_INDEX_AUTH     0
#define QUERY_INDEX_DATA     1

#define PG_CONFIG_NAME "helper"

#define FILE_SERVER_ERROR_MESSAGE "[%s] Error: %s"
//----------------------------------------------------------------------------------------------------------------------

extern "C++" {

namespace Apostol {

    namespace Module {

        //--------------------------------------------------------------------------------------------------------------

        //-- CFileThread -----------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        CFileThread::CFileThread(CFileCommon *AFile, CFileHandler *AHandler, CFileThreadMgr *AThreadMgr):
                CThread(true), CGlobalComponent() {

            m_pFile = AFile;
            m_pHandler = AHandler;
            m_pThreadMgr = AThreadMgr;

            if (Assigned(m_pHandler))
                m_pHandler->SetThread(this);

            if (Assigned(m_pThreadMgr))
                m_pThreadMgr->ActiveThreads().Add(this);
        }
        //--------------------------------------------------------------------------------------------------------------

        CFileThread::~CFileThread() {
            if (Assigned(m_pHandler)) {
                m_pHandler->SetThread(nullptr);
                m_pHandler = nullptr;
            }

            if (Assigned(m_pThreadMgr))
                m_pThreadMgr->ActiveThreads().Remove(this);

            m_pThreadMgr = nullptr;
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileThread::TerminateAndWaitFor() {
            if (FreeOnTerminate())
                throw Delphi::Exception::Exception(_T("Cannot call TerminateAndWaitFor on FreeAndTerminate threads."));

            Terminate();
            if (Suspended())
                Resume();

            WaitFor();
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileThread::Execute() {
            m_pFile->CURL(m_pHandler);
        }

        //--------------------------------------------------------------------------------------------------------------

        //-- CFileThreadMgr --------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        CFileThreadMgr::CFileThreadMgr() {
            m_ThreadPriority = tpNormal;
        }
        //--------------------------------------------------------------------------------------------------------------

        CFileThreadMgr::~CFileThreadMgr() {
            TerminateThreads();
        }
        //--------------------------------------------------------------------------------------------------------------

        CFileThread *CFileThreadMgr::GetThread(CFileCommon *APGFile, CFileHandler *AHandler) {
            return new CFileThread(APGFile, AHandler, this);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileThreadMgr::TerminateThreads() {
            while (m_ActiveThreads.List().Count() > 0) {
                auto pThread = static_cast<CFileThread *> (m_ActiveThreads.List().Last());
                ReleaseThread(pThread);
            }
        }

        //--------------------------------------------------------------------------------------------------------------

        //-- CFileHandler ----------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        CFileHandler::CFileHandler(CQueueCollection *ACollection, const CString &Data, COnQueueHandlerEvent && Handler):
                CQueueHandler(ACollection, static_cast<COnQueueHandlerEvent &&> (Handler)) {

            m_Payload = Data;

            m_Session = m_Payload["session"].AsString();
            m_Operation = m_Payload["operation"].AsString();
            m_FileId = m_Payload["id"].AsString();
            m_Type = m_Payload["type"].AsString();
            m_Path = m_Payload["path"].AsString();
            m_Name = m_Payload["name"].AsString();
            m_Hash = m_Payload["hash"].AsString();

            m_pThread = nullptr;
            m_pConnection = nullptr;

            m_TimeOutInterval = 30 * 60 * 1000;

            UpdateTimeOut(Now());
        }
        //--------------------------------------------------------------------------------------------------------------

        CFileHandler::~CFileHandler() {
            SetConnection(nullptr);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileHandler::SetConnection(CHTTPServerConnection *AConnection) {
            if (m_pConnection != AConnection) {
                if (AConnection != nullptr) {
                    AConnection->Binding(this);
                    AConnection->TimeOut(INFINITE);
                } else {
                    if (m_pConnection != nullptr) {
                        m_pConnection->TimeOutInterval(5 * 1000);
                        m_pConnection->UpdateTimeOut(Now());
                        m_pConnection->Binding(nullptr);
                        m_pConnection->CloseConnection(true);
                    }
                }

                m_pConnection = AConnection;
            }
        }

        //--------------------------------------------------------------------------------------------------------------

        //-- CFileCommon -----------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        CFileCommon::CFileCommon(CModuleProcess *AProcess, const CString &ModuleName, const CString &SectionName):
                CQueueCollection(Config()->PostgresPollMin()), CApostolModule(AProcess, ModuleName, SectionName) {

            m_Headers.Add("Authorization");

            m_Agent = CString().Format("%s (%s)", GApplication->Title().c_str(), ModuleName.c_str());
            m_Host = CApostolModule::GetIPByHostName(CApostolModule::GetHostName());

            m_AuthDate = 0;
            m_Conf = PG_CONFIG_NAME;
        }
        //--------------------------------------------------------------------------------------------------------------

        CPQPollQuery *CFileCommon::ExecuteSQL(const CStringList &SQL, CFileHandler *AHandler,
                COnApostolModuleSuccessEvent &&OnSuccess, COnApostolModuleFailEvent &&OnFail) {

            auto OnExecuted = [this, OnSuccess](CPQPollQuery *APollQuery) {
                auto pHandler = dynamic_cast<CFileHandler *> (APollQuery->Binding());
                auto pConnection = pHandler->Connection();
                if (pConnection != nullptr && pConnection->Connected()) {
                    OnSuccess(pConnection, APollQuery);
                }
                DeleteHandler(pHandler);
            };

            auto OnException = [this, OnFail](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                auto pHandler = dynamic_cast<CFileHandler *> (APollQuery->Binding());
                auto pConnection = pHandler->Connection();
                if (pConnection != nullptr && pConnection->Connected()) {
                    OnFail(pConnection, E);
                }
                DeleteHandler(pHandler);
            };

            AHandler->Lock();
            return ExecSQL(SQL, AHandler, OnExecuted, OnException);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::Authentication() {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {

                CPQueryResults pqResults;
                CStringList SQL;

                try {
                    CApostolModule::QueryToResults(APollQuery, pqResults);

                    const auto &session = pqResults[0].First()["session"];

                    m_Session = pqResults[1].First()["get_session"];

                    m_AuthDate = Now() + (CDateTime) 24 / HoursPerDay;

                    SignOut(session);
                } catch (Delphi::Exception::Exception &E) {
                    DoError(E);
                }
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                DoError(E);
            };

            const auto &caProviders = Server().Providers();
            const auto &caProvider = caProviders.DefaultValue();

            const auto &clientId = caProvider.ClientId(SERVICE_APPLICATION_NAME);
            const auto &clientSecret = caProvider.Secret(SERVICE_APPLICATION_NAME);

            CStringList SQL;

            api::login(SQL, clientId, clientSecret, m_Agent, m_Host);
            api::get_session(SQL, API_BOT_USERNAME, m_Agent, m_Host);

            try {
                ExecSQL(SQL, nullptr, OnExecuted, OnException);
            } catch (Delphi::Exception::Exception &E) {
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::SignOut(const CString &Session) {
            CStringList SQL;

            api::signout(SQL, Session);

            try {
                ExecSQL(SQL);
            } catch (Delphi::Exception::Exception &E) {
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::SendFile(CHTTPServerConnection *AConnection, const CString &FileName) {
            if (AConnection != nullptr && AConnection->Connected()) {
                auto &Reply = AConnection->Reply();

                CString sFileExt;
                TCHAR szBuffer[MAX_BUFFER_SIZE + 1] = {0};

                auto sModified = StrWebTime(FileAge(FileName.c_str()), szBuffer, sizeof(szBuffer));
                if (sModified != nullptr) {
                    AConnection->Reply().AddHeader(_T("Last-Modified"), sModified);
                }

                sFileExt = ExtractFileExt(szBuffer, FileName.c_str());

#if (APOSTOL_USE_SEND_FILE)
    #if (OPENSSL_VERSION_NUMBER >= 0x30000000L) && defined(BIO_get_ktls_send)
                AConnection->SendFileReply(FileName.c_str(), Mapping::ExtToType(sFileExt.c_str()));
    #else
                if (AConnection->IOHandler()->UsedSSL()) {
                    if (Reply.Content.IsEmpty()) {
                        Reply.Content.LoadFromFile(FileName.c_str());
                    }

                    AConnection->SendReply(CHTTPReply::ok, Mapping::ExtToType(sFileExt.c_str()), true);
                } else {
                    AConnection->SendFileReply(FileName.c_str(), Mapping::ExtToType(sFileExt.c_str()));
                }
    #endif
#else
                if (Reply.Content.IsEmpty()) {
                    Reply.Content.LoadFromFile(FileName.c_str());
                }

                AConnection->SendReply(CHTTPReply::ok, Mapping::ExtToType(sFileExt.c_str()), true);
#endif
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::DeleteFile(const CString &FileName) {
            if (FileExists(FileName.c_str())) {
                if (::unlink(FileName.c_str()) == FILE_ERROR) {
                    Log()->Error(APP_LOG_ALERT, errno, _T("could not delete file: \"%s\" error: "), FileName.c_str());
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::DoError(const Delphi::Exception::Exception &E) {
            Log()->Error(APP_LOG_ERR, 0, FILE_SERVER_ERROR_MESSAGE, ModuleName().c_str(), E.what());
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::DoError(CQueueHandler *AHandler, const CString &Message) {
            Log()->Error(APP_LOG_ERR, 0, FILE_SERVER_ERROR_MESSAGE, ModuleName().c_str(), Message.c_str());
            DeleteHandler(AHandler);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::DeleteHandler(CQueueHandler *AHandler) {
            if (Assigned(AHandler)) {
                AHandler->Unlock();
                CQueueCollection::DeleteHandler(AHandler);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::CURL(CFileHandler *AHandler) {

            CCurlFetch curl;
            CURLcode code;

            int count = 0;
            do {
                code = curl.Get(AHandler->URI(), {});
                if (code != CURLE_OK) {
                    sleep(1);
                }
            } while (code != CURLE_OK && count++ < 3);

            auto pConnection = AHandler->Connection();

            CLockGuard Lock(&GFileThreadLock);
            CConnectionLockGuard ConnectionLock(pConnection);

            try {
                if (code == CURLE_OK) {
                    const auto http_code = curl.GetResponseCode();
                    CHTTPReply Reply;

                    Reply.Headers.Clear();
                    for (int i = 1; i < curl.Headers().Count(); i++) {
                        const auto &Header = curl.Headers()[i];
                        Reply.AddHeader(Header.Name(), Header.Value());
                    }

                    Reply.StatusString = http_code;
                    Reply.StatusText = Reply.StatusString;

                    Reply.StringToStatus();

                    if (http_code == 200) {
                        DeleteFile(AHandler->AbsoluteName());

                        Reply.Content = curl.Result();
                        Reply.ContentLength = Reply.Content.Length();

                        Reply.DelHeader("Transfer-Encoding");
                        Reply.DelHeader("Content-Encoding");
                        Reply.DelHeader("Content-Length");

                        Reply.AddHeader("Content-Length", CString::ToString(Reply.ContentLength));

                        Reply.Content.SaveToFile(AHandler->AbsoluteName().c_str());

                        SendFile(pConnection, AHandler->AbsoluteName());

                        DoDone(AHandler, Reply);
                    } else {
                        const CString Message("Not found");
                        ReplyError(pConnection, CHTTPReply::not_found, Message);

                        DoFail(AHandler, Message);
                    }

                    DebugReply(Reply);
                } else {
                    const CString Message(CCurlApi::GetErrorMessage(code));
                    ReplyError(pConnection, CHTTPReply::bad_request, Message);

                    DoFail(AHandler, Message);
                }
            } catch (Delphi::Exception::Exception &E) {
                DoError(E);
                DoFail(AHandler, E.Message());
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::DoDone(CFileHandler *AHandler, const CHTTPReply &Reply) {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {
                auto pHandler = dynamic_cast<CFileHandler *> (APollQuery->Binding());
                DeleteHandler(pHandler);
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                auto pHandler = dynamic_cast<CFileHandler *> (APollQuery->Binding());
                DoError(pHandler, E.Message());
            };

            if (AHandler->Done().IsEmpty()) {
                DeleteHandler(AHandler);
                return;
            }

            const auto &caFileId = PQQuoteLiteral(AHandler->FileId());
            const auto &caAbsoluteName = PQQuoteLiteral(AHandler->AbsoluteName());
            const auto &caHash = SHA256(Reply.Content, true);
            const auto &caContentType = PQQuoteLiteral(Reply.Headers["Content-Type"]);

            CStringList SQL;

            SQL.Add(CString()
                            .MaxFormatSize(256 + caFileId.Size() + caAbsoluteName.Size() + caHash.Size() + caContentType.Size())
                            .Format("SELECT %s(%s::uuid, %s, %d, '%s', %s);",
                                    AHandler->Done().c_str(),
                                    caFileId.c_str(),
                                    caAbsoluteName.c_str(),
                                    Reply.ContentLength,
                                    caHash.c_str(),
                                    caContentType.c_str()
                            ));

            try {
                if (!AHandler->Locked())
                    AHandler->Lock();

                ExecSQL(SQL, AHandler, OnExecuted, OnException);
            } catch (Delphi::Exception::Exception &E) {
                DoError(AHandler, E.Message());
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::DoFail(CFileHandler *AHandler, const CString &Message) {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {
                auto pHandler = dynamic_cast<CFileHandler *> (APollQuery->Binding());
                DeleteHandler(pHandler);
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                auto pHandler = dynamic_cast<CFileHandler *> (APollQuery->Binding());
                DoError(pHandler, E.Message());
            };

            if (AHandler->Fail().IsEmpty()) {
                DeleteHandler(AHandler);
                return;
            }

            const auto &caFileId = PQQuoteLiteral(AHandler->FileId());
            const auto &caMessage = PQQuoteLiteral(Message);

            CStringList SQL;

            SQL.Add(CString()
                            .MaxFormatSize(256 + caFileId.Size() + caMessage.Size())
                            .Format("SELECT %s(%s::uuid, %s);",
                                    AHandler->Fail().c_str(),
                                    caFileId.c_str(),
                                    caMessage.c_str()
                            ));

            try {
                if (!AHandler->Locked())
                    AHandler->Lock();

                ExecSQL(SQL, AHandler, OnExecuted, OnException);
            } catch (Delphi::Exception::Exception &E) {
                DoError(AHandler, E.Message());
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::DoFetch(CQueueHandler *AHandler) {

            auto OnRequest = [AHandler](CHTTPClient *Sender, CHTTPRequest &Request) {
                auto pHandler = dynamic_cast<CFileHandler *> (AHandler);

                if (Assigned(pHandler)) {
                    CLocation URI(pHandler->URI());

                    CHTTPRequest::Prepare(Request, "GET", URI.href().c_str());
                }

                DebugRequest(Request);
            };
            //----------------------------------------------------------------------------------------------------------

            auto OnExecute = [this, AHandler](CTCPConnection *Sender) {
                auto pConnection = dynamic_cast<CHTTPClientConnection *> (Sender);
                auto &Reply = pConnection->Reply();

                DebugReply(Reply);

                auto pHandler = dynamic_cast<CFileHandler *> (AHandler);
                if (Assigned(pHandler)) {
                    auto pHandlerConnection = pHandler->Connection();

                    if (Reply.Status == CHTTPReply::ok) {
                        try {
                            Reply.Content.SaveToFile(pHandler->AbsoluteName().c_str());
                            SendFile(pHandlerConnection, pHandler->AbsoluteName());
                        } catch (Delphi::Exception::Exception &E) {
                            DoError(E);
                        }

                        DoDone(pHandler, Reply);
                    } else {
                        const CString Message("Not found");
                        ReplyError(pHandlerConnection, CHTTPReply::not_found, Message);

                        DoFail(pHandler, Message);
                    }
                }

                return true;
            };
            //----------------------------------------------------------------------------------------------------------

            auto OnException = [this, AHandler](CTCPConnection *Sender, const Delphi::Exception::Exception &E) {
                auto pConnection = dynamic_cast<CHTTPClientConnection *> (Sender);
                DebugReply(pConnection->Reply());

                auto pHandler = dynamic_cast<CFileHandler *> (AHandler);
                if (Assigned(pHandler)) {
                    auto pHandlerConnection = pHandler->Connection();

                    const CString Message("Not found");
                    ReplyError(pHandlerConnection, CHTTPReply::not_found, Message);

                    DoFail(pHandler, E.what());
                }

                DoError(E);
            };
            //----------------------------------------------------------------------------------------------------------

            auto pHandler = dynamic_cast<CFileHandler *> (AHandler);

            if (pHandler == nullptr)
                return;

            pHandler->Lock();
            pHandler->Allow(false);
            pHandler->TimeOut(0);
            pHandler->UpdateTimeOut(Now());

            const auto &caPayload = pHandler->Payload();

            CLocation URI(pHandler->URI());

            auto pClient = GetClient(URI.hostname, URI.port);
#if defined(_GLIBCXX_RELEASE) && (_GLIBCXX_RELEASE >= 9)
            pClient->OnConnected([this](auto &&Sender) { DoClientConnected(Sender); });
            pClient->OnDisconnected([this](auto &&Sender) { DoClientDisconnected(Sender); });
#else
            pClient->OnConnected(std::bind(&CFileCommon::DoClientConnected, this, _1));
            pClient->OnDisconnected(std::bind(&CFileCommon::DoClientDisconnected, this, _1));
#endif

            pClient->OnRequest(OnRequest);
            pClient->OnExecute(OnExecute);
            pClient->OnException(OnException);

            try {
                pClient->AutoFree(true);
                pClient->Active(true);
            } catch (std::exception &e) {
                pHandler->Unlock();
                DoFail(pHandler, e.what());
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::DoLink(CQueueHandler *AHandler) {
            try {
                auto pThread = GetThread(dynamic_cast<CFileHandler *> (AHandler));

                if (AHandler->Allow()) {
                    AHandler->Lock();
                    AHandler->Allow(false);

                    IncProgress();
                }

                pThread->FreeOnTerminate(true);
                pThread->Resume();
            } catch (std::exception &e) {
                DoError(AHandler, e.what());
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::DoPostgresQueryExecuted(CPQPollQuery *APollQuery) {
            CPQResult *pResult;

            try {
                for (int i = 0; i < APollQuery->Count(); i++) {
                    pResult = APollQuery->Results(i);
                    if (pResult->ExecStatus() != PGRES_TUPLES_OK)
                        throw Delphi::Exception::EDBError(pResult->GetErrorMessage());
                }
            } catch (Delphi::Exception::Exception &E) {
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::DoPostgresQueryException(CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
            DoError(E);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::DoClientConnected(CObject *Sender) {
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

        void CFileCommon::DoClientDisconnected(CObject *Sender) {
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

        void CFileCommon::UnloadQueue() {
            const auto index = m_Queue.IndexOf(this);
            if (index != -1) {
                const auto pQueue = m_Queue[index];
                for (int i = 0; i < pQueue->Count(); ++i) {
                    auto pHandler = (CFileHandler *) pQueue->Item(i);
                    if (pHandler != nullptr && pHandler->Allow()) {
                        pHandler->Handler();
                        if (m_Progress >= m_MaxQueue) {
                            break;
                        }
                    }
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::CheckTimeOut(CDateTime Now) {
            const auto index = m_Queue.IndexOf(this);
            if (index != -1) {
                const auto pQueue = m_Queue[index];
                for (int i = pQueue->Count() - 1; i >= 0; i--) {
                    auto pHandler = (CFileHandler *) pQueue->Item(i);
                    if (pHandler != nullptr && !pHandler->Allow()) {
                        if ((pHandler->TimeOut() != INFINITE) && (Now >= pHandler->TimeOut())) {
                            DoFail(pHandler, "Killed by timeout");
                        }
                    }
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        CPQPollQuery *CFileCommon::GetQuery(CPollConnection *AConnection) {
            CPQPollQuery *pQuery = m_pModuleProcess->GetQuery(AConnection, m_Conf);

            if (Assigned(pQuery)) {
#if defined(_GLIBCXX_RELEASE) && (_GLIBCXX_RELEASE >= 9)
                pQuery->OnPollExecuted([this](auto && APollQuery) { DoPostgresQueryExecuted(APollQuery); });
                pQuery->OnException([this](auto && APollQuery, auto && AException) { DoPostgresQueryException(APollQuery, AException); });
#else
                pQuery->OnPollExecuted(std::bind(&CFileCommon::DoPostgresQueryExecuted, this, _1));
                pQuery->OnException(std::bind(&CFileCommon::DoPostgresQueryException, this, _1, _2));
#endif
            }

            return pQuery;
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::Initialization(CModuleProcess *AProcess) {
            m_Path = Config()->IniFile().ReadString(SectionName().c_str(), "path", "files/");
            m_Type = Config()->IniFile().ReadString(SectionName().c_str(), "type", "native");

            if (!path_separator(m_Path.front())) {
                m_Path = Config()->Prefix() + m_Path;
            }

            if (!path_separator(m_Path.back())) {
                m_Path = m_Path + "/";
            }

            ForceDirectories(m_Path.c_str(), 0755);
        }
        //--------------------------------------------------------------------------------------------------------------

        CFileThread *CFileCommon::GetThread(CFileHandler *AHandler) {
            return m_ThreadMgr.GetThread(this, AHandler);
        }
        //--------------------------------------------------------------------------------------------------------------

    }
}
}