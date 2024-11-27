/*++

Program name:

  Apostol CRM

Module Name:

  FetchCommon.hpp

Notices:

  Module: Postgres Fetch Common

Author:

  Copyright (c) Prepodobny Alen

  mailto: alienufo@inbox.ru
  mailto: ufocomp@gmail.com

--*/

#ifndef APOSTOL_FETCH_COMMON_HPP
#define APOSTOL_FETCH_COMMON_HPP
//----------------------------------------------------------------------------------------------------------------------

extern "C++" {

namespace Apostol {

    namespace Module {

        class CFetchCommon;

        //--------------------------------------------------------------------------------------------------------------

        //-- CFetchHandler ---------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        class CFetchHandler: public CQueueHandler {
        private:

            CString m_RequestId;

            CJSON m_Payload;

        public:

            CFetchHandler(CQueueCollection *ACollection, const CString &RequestId, COnQueueHandlerEvent && Handler);

            ~CFetchHandler() override = default;

            const CString &RequestId() const { return m_RequestId; }

            CJSON &Payload() { return m_Payload; }
            const CJSON &Payload() const { return m_Payload; }

        };

        //--------------------------------------------------------------------------------------------------------------

        //-- CFetchCommon ----------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        class CFetchCommon: public CQueueCollection, public CApostolModule {
        private:

        protected:

            int m_TimeOut;

            void InitMethods() override {};

            void CheckTimeOut(CDateTime Now);

            static CJSON ParamsToJson(const CStringList &Params);
            static CJSON HeadersToJson(const CHeaders &Headers);

            static void QueryException(CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E);

            static int CheckError(const CJSON &Json, CString &ErrorMessage);
            static CHTTPReply::CStatusType ErrorCodeToStatus(int ErrorCode);

            void DeleteHandler(CQueueHandler *AHandler) override;

            void DoError(const Delphi::Exception::Exception &E) const;

            void DoDone(CFetchHandler *AHandler, const CHTTPReply &Reply);
            void DoFail(CFetchHandler *AHandler, const CString &Message);
            void DoStream(CFetchHandler *AHandler, const CString &Data);

            void DoPostgresQueryExecuted(CPQPollQuery *APollQuery) override;
            void DoPostgresQueryException(CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) override;

            void DoConnected(CObject *Sender) const;
            void DoDisconnected(CObject *Sender) const;

        public:

            explicit CFetchCommon(CModuleProcess *AProcess, const CString &ModuleName, const CString &SectionName);

            ~CFetchCommon() override = default;

            void UnloadQueue() override;

        };

    }
}

using namespace Apostol::Module;
}
#endif //APOSTOL_FETCH_COMMON_HPP
