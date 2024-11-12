package br.com.sankhya.SUPERFOOD.acoes;

import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.extensions.actionbutton.Registro;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.jape.wrapper.fluid.FluidCreateVO;
import br.com.sankhya.modelcore.util.DynamicEntityNames;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class ControleRun implements AcaoRotinaJava {
    @Override
    public void doAction(ContextoAcao contexto) throws Exception {
        JdbcWrapper jdbc = null;
        BigDecimal pkTelaRam = BigDecimal.valueOf((long) (Integer) contexto.getParam("PK_TELA_RAM"));
        Registro reg = contexto.getLinhas()[0];
        BigDecimal top = (BigDecimal) reg.getCampo("CODTIPOPER");
        BigDecimal nuNota = (BigDecimal) reg.getCampo("NUNOTA");
        Timestamp dtEntSai = (Timestamp) reg.getCampo("DTENTSAI");
        BigDecimal vlrTotal = null;
        Integer evento = 44;
        SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm");
        String formattedDate = dateFormat.format(new Date());

        if (top.compareTo(new BigDecimal(1307)) != 0) {
            return;
        }

        jdbc = EntityFacadeFactory.getDWFFacade().getJdbcWrapper();
        jdbc.openSession();
        ResultSet rs = null;
        ResultSet rs2 = null;

        try {
            NativeSql sqlUsuarioLogado = new NativeSql(jdbc);
            sqlUsuarioLogado.appendSql("SELECT STP_GET_CODUSULOGADO() AS CODUSULOGADO FROM DUAL");
            BigDecimal codUsuSolic = null;
            rs = sqlUsuarioLogado.executeQuery();
            if (rs.next()) {
                codUsuSolic = rs.getBigDecimal("CODUSULOGADO");
            }

            if (codUsuSolic == null) {
                throw new Exception("Erro ao obter o código do usuário logado.");
            }

            NativeSql sqlTGFITE = new NativeSql(jdbc);
            sqlTGFITE.appendSql("SELECT CODPROD, VLRUNIT, VLRTOT, NUNOTA FROM TGFITE WHERE NUNOTA = :NUNOTA");
            sqlTGFITE.setNamedParameter("NUNOTA", nuNota);
            rs = sqlTGFITE.executeQuery();

            while (rs.next()) {
                BigDecimal codProd = rs.getBigDecimal("CODPROD");
                BigDecimal vlrUnit = rs.getBigDecimal("VLRUNIT");
                vlrTotal = rs.getBigDecimal("VLRTOT");
                nuNota = rs.getBigDecimal("NUNOTA");

                if (vlrUnit == null) {
                    throw new Exception("Valor unitário (VLRUNIT) está nulo para o produto " + codProd);
                }

                NativeSql sqlPrecRam = new NativeSql(jdbc);
                sqlPrecRam.appendSql("SELECT VALORPERIODO, DTINICIO, DTFIM FROM AD_TGFPRECORAM WHERE SEQ = :SEQ AND CODPROD = :CODPROD");
                sqlPrecRam.setNamedParameter("CODPROD", codProd);
                sqlPrecRam.setNamedParameter("SEQ", pkTelaRam);
                rs2 = sqlPrecRam.executeQuery();

                boolean dentroDoPeriodo = false;

                while (rs2.next()) {
                    BigDecimal valorPeriodo = rs2.getBigDecimal("VALORPERIODO");
                    Timestamp dtInicio = rs2.getTimestamp("DTINICIO");
                    Timestamp dtFim = rs2.getTimestamp("DTFIM");

                    dentroDoPeriodo = dtEntSai.after(dtInicio) && dtEntSai.before(dtFim);

                    if (!dentroDoPeriodo || vlrUnit.compareTo(valorPeriodo) > 0) {
                        try {
                            JapeWrapper cicloDAO = JapeFactory.dao(DynamicEntityNames.LIBERACAO_LIMITE);
                            FluidCreateVO fluidCreateVO = cicloDAO.create();
                            fluidCreateVO.set("VLRLIBERADO", BigDecimal.ZERO);
                            fluidCreateVO.set("VLRATUAL", vlrTotal);
                            fluidCreateVO.set("NUCHAVE", nuNota);
                            fluidCreateVO.set("EVENTO", BigDecimal.valueOf(evento));
                            fluidCreateVO.set("TABELA", "TGFCAB");
                            fluidCreateVO.set("CODUSULIB", BigDecimal.valueOf(23));
                            fluidCreateVO.set("DHLIB", Timestamp.valueOf(LocalDateTime.parse(formattedDate, DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm"))));
                            fluidCreateVO.set("DHSOLICIT", Timestamp.valueOf(LocalDateTime.parse(formattedDate, DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm"))));
                            fluidCreateVO.set("VLRLIMITE", new BigDecimal("1000000000.00"));
                            fluidCreateVO.set("CODUSUSOLICIT", codUsuSolic);
                            fluidCreateVO.save();

                        } catch (Exception e) {
                            e.printStackTrace();
                            throw new RuntimeException("Erro ao atualizar limite de liberação da nota fiscal. " + e.getMessage());
                        }
                        contexto.setMensagemRetorno("O Produto: " + codProd
                                + " está fora do período permitido ou com valor acima do valor permitido para o período. Necessário solicitar liberação ao Bruno Campanharo.");
                    } else {

                        contexto.setMensagemRetorno("Sucesso: Todos os produtos estão dentro do valor e do período permitido.");
                    }
                }
                rs2.close();
            }

        } catch (Exception e) {
            throw new Exception("Erro na validação de preço ou data do produto: " + e.getMessage(), e);
        } finally {
            if (rs != null) rs.close();
            jdbc.closeSession();
        }
    }
}